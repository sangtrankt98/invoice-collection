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
    For each message_id, determine the dominant entity based on tax number and frequency
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

        # If tax number approach doesn't work, count entity occurrences
        entity_counts = Counter(group["entity_name"].dropna())

        if not entity_counts:
            dominant_entities[message_id] = {"entity": None, "tax_number": None}
            continue

        # Find the most common entities
        most_common_entities = entity_counts.most_common()

        # Check if there's a tie for the most common entity
        if (
            len(most_common_entities) > 1
            and most_common_entities[0][1] == most_common_entities[1][1]
        ):
            # We have a tie! Try to resolve using multiple strategies

            # Strategy 1: Prefer the entity that appears in the "from_email" or "subject"
            from_email = (
                group["from_email"].iloc[0] if not group["from_email"].empty else ""
            )
            subject = group["subject"].iloc[0] if not group["subject"].empty else ""
            email_text = (from_email + " " + subject).upper()

            for entity_name, _ in most_common_entities:
                normalized_entity = normalize_company_name(entity_name)
                # Check if any part of the normalized entity name appears in the email text
                entity_words = normalized_entity.split()
                for word in entity_words:
                    if (
                        len(word) > 3 and word in email_text
                    ):  # Only consider words longer than 3 chars
                        dominant_entity = entity_name
                        # Get tax number for this entity
                        tax_number = (
                            group[group["entity_name"] == dominant_entity][
                                "entity_tax_number"
                            ]
                            .dropna()
                            .iloc[0]
                            if not group[group["entity_name"] == dominant_entity][
                                "entity_tax_number"
                            ]
                            .dropna()
                            .empty
                            else None
                        )
                        dominant_entities[message_id] = {
                            "entity": dominant_entity,
                            "tax_number": tax_number,
                        }
                        break

            # Strategy 2: If still tied, prefer the entity with the most financial transactions
            if message_id not in dominant_entities:
                # Calculate total financial activity (sum of total_amount) for each entity
                entity_financials = {}
                for entity_name, _ in most_common_entities:
                    entity_rows = group[group["entity_name"] == entity_name]
                    total_amount = entity_rows["total_amount"].sum()
                    entity_financials[entity_name] = total_amount

                # Find entity with highest financial activity
                if entity_financials:
                    dominant_entity = max(
                        entity_financials.items(), key=lambda x: x[1]
                    )[0]
                    tax_number = (
                        group[group["entity_name"] == dominant_entity][
                            "entity_tax_number"
                        ]
                        .dropna()
                        .iloc[0]
                        if not group[group["entity_name"] == dominant_entity][
                            "entity_tax_number"
                        ]
                        .dropna()
                        .empty
                        else None
                    )
                    dominant_entities[message_id] = {
                        "entity": dominant_entity,
                        "tax_number": tax_number,
                    }
                    continue

            # Strategy 3: If still tied, use document counts
            if message_id not in dominant_entities:
                # Count document types for each entity
                entity_doc_counts = {}
                for entity_name, _ in most_common_entities:
                    entity_rows = group[group["entity_name"] == entity_name]
                    doc_count = len(entity_rows)
                    entity_doc_counts[entity_name] = doc_count

                if entity_doc_counts:
                    dominant_entity = max(
                        entity_doc_counts.items(), key=lambda x: x[1]
                    )[0]
                    tax_number = (
                        group[group["entity_name"] == dominant_entity][
                            "entity_tax_number"
                        ]
                        .dropna()
                        .iloc[0]
                        if not group[group["entity_name"] == dominant_entity][
                            "entity_tax_number"
                        ]
                        .dropna()
                        .empty
                        else None
                    )
                    dominant_entities[message_id] = {
                        "entity": dominant_entity,
                        "tax_number": tax_number,
                    }
                    continue

            # If we still don't have a resolution, just pick the first one
            if message_id not in dominant_entities:
                dominant_entity = most_common_entities[0][0]
                tax_number = (
                    group[group["entity_name"] == dominant_entity]["entity_tax_number"]
                    .dropna()
                    .iloc[0]
                    if not group[group["entity_name"] == dominant_entity][
                        "entity_tax_number"
                    ]
                    .dropna()
                    .empty
                    else None
                )
                dominant_entities[message_id] = {
                    "entity": dominant_entity,
                    "tax_number": tax_number,
                }
        else:
            # No tie, use the most common entity
            dominant_entity = most_common_entities[0][0]
            tax_number = (
                group[group["entity_name"] == dominant_entity]["entity_tax_number"]
                .dropna()
                .iloc[0]
                if not group[group["entity_name"] == dominant_entity][
                    "entity_tax_number"
                ]
                .dropna()
                .empty
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

    # Find the dominant entity for each message_id (includes tax number)
    dominant_entities = find_dominant_entity_by_message_id(processed_df)

    # Create new columns for dominant entity and tax number
    processed_df["message_dominant_entity"] = processed_df["message_id"].apply(
        lambda mid: dominant_entities.get(mid, {}).get("entity")
    )
    processed_df["message_dominant_tax"] = processed_df["message_id"].apply(
        lambda mid: dominant_entities.get(mid, {}).get("tax_number")
    )

    # Update entity_name to be the dominant entity for that email
    processed_df["entity_name"] = processed_df.apply(
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
                "entity_name": row["entity_name_orig"],
                "counterparty_name": row["counterparty_name"],
                "entity_tax_number": row["entity_tax_number"],
                "counterparty_tax_number": row["counterparty_tax_number"],
            },
            {
                "entity": row["message_dominant_entity"],
                "tax_number": row["message_dominant_tax"],
            },
        ),
        axis=1,
    )

    # Clean up the dataframe - remove ALL temporary columns
    temp_columns = [
        "entity_name_orig",
        "counterparty_name_orig",
        "message_dominant_entity",
        "message_dominant_tax",
    ]

    # Only drop columns that actually exist
    columns_to_drop = [col for col in temp_columns if col in processed_df.columns]
    if columns_to_drop:
        processed_df = processed_df.drop(columns=columns_to_drop)

    return processed_df
