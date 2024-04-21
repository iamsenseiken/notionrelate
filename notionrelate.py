import argparse
import time
from notion_client import Client

def fetch_all_pages(notion, database_id, property_name):
    result = {}
    has_more = True
    start_cursor = None
    while has_more:
        response = notion.databases.query(
            database_id=database_id, start_cursor=start_cursor
        )
        for page in response["results"]:
            try:
                property_data = page["properties"].get(property_name, {})
                if "title" in property_data:
                    property_value = property_data["title"][0]["plain_text"]
                elif "rich_text" in property_data:
                    property_value = property_data["rich_text"][0]["plain_text"]
                elif "number" in property_data:
                    property_value = str(property_data["number"])
                else:
                    property_value = None

                if property_value:
                    if property_value not in result:
                        result[property_value] = []
                    result[property_value].append(page["id"])
            except (IndexError, KeyError) as e:
                print(f"Error processing page properties: {e}")

        has_more = response["has_more"]
        start_cursor = response.get("next_cursor")
    return result

def link_records(
    notion,
    base_db_id,
    aux_db_id,
    match_property,
    match_property_in_aux,
    link_property,
    dryrun,
    skip,
    verbose,
    max_records=None
):
    change_count = 0
    aux_records = fetch_all_pages(notion, aux_db_id, match_property_in_aux)
    has_more = True
    start_cursor = None
    processed_count = 0
    dry_run_msg = "Dry run: " if dryrun else ""

    while has_more and (max_records is None or processed_count < max_records):
        response = notion.databases.query(
            database_id=base_db_id, start_cursor=start_cursor
        )
        for base_record in response["results"]:
            if max_records is not None and processed_count >= max_records:
                break

            try:
                property_data = base_record["properties"].get(match_property, {})
                match_value = (
                    property_data.get("title")
                    or property_data.get("rich_text")
                    or property_data.get("number")
                    or [{}]
                )[0].get("plain_text", str(property_data.get("number")))
            except (IndexError, KeyError) as e:
                print(f"Error processing match property: {e}")
                continue

            if match_value:
                aux_record_ids = aux_records.get(match_value, [])
                existing_relations = (
                    base_record["properties"].get(link_property, {}).get("relation", [])
                )
                if aux_record_ids and (not existing_relations or not skip):
                    relation_updates = [{"id": id} for id in aux_record_ids]
                    change_count += len(aux_record_ids)

                    if verbose:
                        print(
                            f"{dry_run_msg}Linking Base Record ID {base_record['id']} with Auxiliary Record IDs {aux_record_ids} in {link_property}"
                        )
                    else:
                        print(f"#", end="")

                    if not dryrun:
                        notion.pages.update(
                            page_id=base_record["id"],
                            properties={link_property: {"relation": relation_updates}},
                        )

                elif existing_relations and skip:
                    if verbose:
                        print(
                            f"Skipping Base Record ID {base_record['id']} as it already has a relation in {link_property}."
                        )
                    else:
                        print(f">", end="")
                else:
                    if verbose:
                        print(
                            f"No matching records found in auxiliary database for Base Record ID {base_record['id']} with value {match_value}."
                        )
                    else:
                        print(f"_", end="")
            
            processed_count += 1

        has_more = response["has_more"]
        start_cursor = response.get("next_cursor")

    if not verbose:
        print(f"")

    return change_count

def main():
    parser = argparse.ArgumentParser(description="Link records in Notion databases based on a common property.")
    parser.add_argument("--token", required=True, help="Notion integration token")
    parser.add_argument("--base", required=True, help="Base database ID")
    parser.add_argument("--aux", required=True, help="Auxiliary database ID to pull relations from")
    parser.add_argument("--field", required=True, help="Property name used for matching records in the base database")
    parser.add_argument("--match", required=True, help="Property name used for finding matches in the auxiliary database")
    parser.add_argument("--link", required=True, help="Property name in the base database where the relation will be created")
    parser.add_argument("--dryrun", action='store_true', help="Run the script in dry run mode to see what changes would be made without making them")
    parser.add_argument("--skip", action='store_true', help="Skip updating records that already have a related entry")
    parser.add_argument("--verbose", action='store_true', help="Output detailed logs of all actions")
    parser.add_argument("--max", type=int, help="Maximum number of records to process")

    args = parser.parse_args()

    start_time = time.time()

    print("Running with parameters:")
    print(f"Base ID: {args.base}")
    print(f"Aux ID: {args.aux}")
    print(f"Match Field: {args.field}")
    print(f"Link Field: {args.link}")
    print(f"Dry Run: {args.dryrun}")
    print(f"Skip Existing: {args.skip}")

    notion = Client(auth=args.token)
    changes = link_records(notion, args.base, args.aux, args.field, args.match, args.link, args.dryrun, args.skip, args.verbose, args.max)
    print(f"Total changes made or would have been made: {changes}")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total processing time: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()
