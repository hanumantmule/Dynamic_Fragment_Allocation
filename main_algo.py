from fragment_reallocation import *


# ---------------------------------- Main Flow -----------------------------------------
def main():
    # Horizontal fragmentation and initial allocation.
    initial_allocation = {'F1': 'S1', 'F2': 'S4', 'F3': 'S3', 'F4': 'S2',
                          'F5': 'S1', 'F6': 'S4', 'F7': 'S2', 'F8': 'S3'}
    initial_setup(initial_allocation)

    # insert_employee()

    # Fetch Existing static fragment -> site mapping structure into dictionary
    site_list = fetch_distinct_sites()
    site_fragment_static_dict = fetch_static_fragment_alloc()
    print("Site and Fragment Mapping:")
    for s, k in site_fragment_static_dict.items():
        print(s, k, '\n')

    # Generate random query access log data.
    for site_name, fragment_list in site_fragment_static_dict.items():
        insert_log_data(fragment_list, site_name, site_list)

    # Obtain and Analyze the query access log for each site
    site_fragment_reallocation_dict = fragment_reallocation_main(site_fragment_static_dict)
    print("\n Fragments and site mapping after reallocation")
    for s, k in site_fragment_reallocation_dict.items():
        print(s, '-->', k, '\n')

    # Update fragment allocation table as per new reallocation output
    update_fragment_reallocation(site_fragment_reallocation_dict)


if __name__ == "__main__":
    main()
