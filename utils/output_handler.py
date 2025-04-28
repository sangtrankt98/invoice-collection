#!/usr/bin/env python3
"""
Output Handler Module
Contains utilities for processing and standardizing output data from invoice collection
"""
import re
import pandas as pd
from collections import Counter


def find_dominant_entity_by_message_id(df):
    """
    For each message_id, determine the dominant entity based on both entity and counterparty columns
    Returns a dict mapping message_id to its dominant entity and tax number
    """
    dominant_entities = {}

    # Group by message_id
    for message_id, group in df.groupby("message_id"):
        # First check if there's a consistent entity_tax_number in this group
        tax_numbers = group["entity_tax_number"].dropna().unique()

        if len(tax_numbers) == 1:
            # If there's exactly one tax number, use it to find the corresponding entity
            tax_number = tax_numbers[0]
            # Filter to rows with this tax number and get the most common entity name
            entities = group[group["entity_tax_number"] == tax_number][
                "entity_name"
            ].dropna()
            if not entities.empty:
                dominant_entity = entities.mode().iloc[0]
                dominant_entities[message_id] = {
                    "entity": dominant_entity,
                    "tax_number": tax_number,
                }
                continue

        # NEW APPROACH: Analyze both entity_name and counterparty_name columns
        entity_counts = Counter(group["entity_name"].dropna())
        counterparty_counts = Counter(group["counterparty_name"].dropna())

        # Combine the counts from both columns to find potential dominant entities
        all_companies = set(entity_counts.keys()) | set(counterparty_counts.keys())
        company_totals = {}

        for company in all_companies:
            # Count total appearances across both columns
            total_appearances = entity_counts.get(company, 0) + counterparty_counts.get(
                company, 0
            )
            company_totals[company] = total_appearances

        # Get companies with the highest total appearances
        if not company_totals:
            dominant_entities[message_id] = {"entity": None, "tax_number": None}
            continue

        max_appearances = max(company_totals.values())
        top_companies = [
            c for c, count in company_totals.items() if count == max_appearances
        ]

        # If we have multiple top companies, use a weighted approach to decide
        if len(top_companies) > 1:
            # Calculate a score for each company:
            # - Give more weight to appearing as an entity (seller) than counterparty (buyer)
            # - Add financial volume as a tiebreaker
            company_scores = {}

            for company in top_companies:
                # Base score is total appearances
                score = company_totals[company]

                # Add bonus for entity appearances (seller role)
                entity_bonus = (
                    entity_counts.get(company, 0) * 1.5
                )  # 50% more weight for seller role
                score += entity_bonus

                # Add financial activity bonus
                # Sum transactions where company is entity
                entity_rows = group[group["entity_name"] == company]
                entity_amount = (
                    entity_rows["total_amount"].sum()
                    if "total_amount" in group.columns
                    else 0
                )

                # Sum transactions where company is counterparty
                counterparty_rows = group[group["counterparty_name"] == company]
                counterparty_amount = (
                    counterparty_rows["total_amount"].sum()
                    if "total_amount" in group.columns
                    else 0
                )

                # Companies that sell more (higher entity_amount) get priority
                financial_score = entity_amount - counterparty_amount
                # Normalize financial score to have less impact than appearance counts
                if financial_score != 0:
                    financial_bonus = 0.5 * (
                        financial_score / abs(financial_score)
                    )  # +0.5 or -0.5 based on direction
                    score += financial_bonus

                company_scores[company] = score

            # Get company with highest score
            dominant_entity = max(company_scores.items(), key=lambda x: x[1])[0]
        else:
            # Just one top company
            dominant_entity = top_companies[0]

        # Get tax number for this entity
        tax_rows = group[group["entity_name"] == dominant_entity][
            "entity_tax_number"
        ].dropna()
        if not tax_rows.empty:
            tax_number = tax_rows.iloc[0]
        else:
            # Check if tax number exists in counterparty rows
            counterparty_tax_rows = group[
                group["counterparty_name"] == dominant_entity
            ]["counterparty_tax_number"].dropna()
            tax_number = (
                counterparty_tax_rows.iloc[0]
                if not counterparty_tax_rows.empty
                else None
            )

        dominant_entities[message_id] = {
            "entity": dominant_entity,
            "tax_number": tax_number,
        }

    return dominant_entities


def determine_transaction_direction(row, target_entity_info):
    """
    Determine if a transaction is incoming or outgoing based on entity and counterparty

    Args:
        row: Dictionary containing entity_name, counterparty_name, entity_tax_number, counterparty_tax_number
        target_entity_info: Dictionary with 'entity' (name) and 'tax_number'

    Returns:
        Direction string: "INCOMING", "OUTGOING", "INTERNAL", or None
    """
    # Skip if essential data is missing
    if not isinstance(row.get("entity_name"), str) or not isinstance(
        row.get("counterparty_name"), str
    ):
        return None

    if not target_entity_info or not target_entity_info.get("entity"):
        return None

    target_entity = target_entity_info.get("entity")
    target_tax_number = target_entity_info.get("tax_number")

    entity_normalized = row.get("entity_name")
    counterparty_normalized = row.get("counterparty_name")
    target_normalized = target_entity

    # First check tax numbers if available
    entity_tax = row.get("entity_tax_number")
    counterparty_tax = row.get("counterparty_tax_number")

    # Use tax numbers for the most accurate determination
    if target_tax_number is not None:
        if entity_tax == target_tax_number and counterparty_tax == target_tax_number:
            return "INTERNAL"
        elif entity_tax == target_tax_number:
            return "OUTGOING"
        elif counterparty_tax == target_tax_number:
            return "INCOMING"

    # Fall back to name-based determination if tax numbers don't resolve
    if entity_normalized == target_normalized:
        if counterparty_normalized == target_normalized:
            return "INTERNAL"
        return "OUTGOING"

    if counterparty_normalized == target_normalized:
        return "INCOMING"

    # Can't determine
    return None


def standardize_dataframe(df):
    """
    Standardize a dataframe with entity information:
    1. Identify dominant entities by message
    2. Set consistent entity names
    3. Determine transaction directions
    4. Swap entity and counterparty when needed to maintain consistency

    Args:
        df: DataFrame containing invoice/transaction data

    Returns:
        Processed DataFrame with standardized entities and directions
    """
    # Work on a copy to avoid modifying the original
    processed_df = df.copy()

    # Save original entity and counterparty for reference
    processed_df["entity_name_orig"] = processed_df["entity_name"]
    processed_df["counterparty_name_orig"] = processed_df["counterparty_name"]
    processed_df["entity_tax_number_orig"] = processed_df["entity_tax_number"]
    processed_df["counterparty_tax_number_orig"] = processed_df[
        "counterparty_tax_number"
    ]

    # Find the dominant entity for each message_id (includes tax number)
    dominant_entities = find_dominant_entity_by_message_id(processed_df)

    # Create new columns for dominant entity and tax number
    processed_df["message_dominant_entity"] = processed_df["message_id"].apply(
        lambda mid: dominant_entities.get(mid, {}).get("entity")
    )
    processed_df["message_dominant_tax"] = processed_df["message_id"].apply(
        lambda mid: dominant_entities.get(mid, {}).get("tax_number")
    )

    # For each row, check if we need to swap entity and counterparty
    processed_df["needs_swap"] = processed_df.apply(
        lambda row: (
            pd.notnull(row["message_dominant_entity"])
            and pd.notnull(row["counterparty_name"])
            and row["counterparty_name"] == row["message_dominant_entity"]
        ),
        axis=1,
    )

    # Perform the swap for rows that need it
    for idx, row in processed_df[processed_df["needs_swap"]].iterrows():
        # Swap entity and counterparty names
        processed_df.at[idx, "entity_name"] = row["counterparty_name"]
        processed_df.at[idx, "counterparty_name"] = row["entity_name_orig"]

        # Swap tax numbers if present
        if pd.notnull(row["entity_tax_number"]) or pd.notnull(
            row["counterparty_tax_number"]
        ):
            processed_df.at[idx, "entity_tax_number"] = row["counterparty_tax_number"]
            processed_df.at[idx, "counterparty_tax_number"] = row[
                "entity_tax_number_orig"
            ]

    # For rows that don't need swapping, set entity_name to dominant entity
    processed_df.loc[~processed_df["needs_swap"], "entity_name"] = processed_df.apply(
        lambda row: (
            row["message_dominant_entity"]
            if pd.notnull(row["message_dominant_entity"])
            else row["entity_name"]
        ),
        axis=1,
    )

    # Determine transaction direction using both entity names and tax numbers
    processed_df["direction"] = processed_df.apply(
        lambda row: determine_transaction_direction(
            {
                "entity_name": (
                    row["entity_name_orig"]
                    if not row["needs_swap"]
                    else row["counterparty_name_orig"]
                ),
                "counterparty_name": (
                    row["counterparty_name_orig"]
                    if not row["needs_swap"]
                    else row["entity_name_orig"]
                ),
                "entity_tax_number": (
                    row["entity_tax_number_orig"]
                    if not row["needs_swap"]
                    else row["counterparty_tax_number_orig"]
                ),
                "counterparty_tax_number": (
                    row["counterparty_tax_number_orig"]
                    if not row["needs_swap"]
                    else row["entity_tax_number_orig"]
                ),
            },
            {
                "entity": row["message_dominant_entity"],
                "tax_number": row["message_dominant_tax"],
            },
        ),
        axis=1,
    )

    # Clean up the dataframe - remove temporary columns
    temp_columns = [
        "entity_name_orig",
        "counterparty_name_orig",
        "entity_tax_number_orig",
        "counterparty_tax_number_orig",
        "message_dominant_entity",
        "message_dominant_tax",
        "needs_swap",
    ]

    # Only drop columns that actually exist
    columns_to_drop = [col for col in temp_columns if col in processed_df.columns]
    if columns_to_drop:
        processed_df = processed_df.drop(columns=columns_to_drop)

    return processed_df
