from data_setup import *


def fetch_avg_vol_from_all_sites(query, fragment_name):
    cur = conn.cursor()
    cur.execute(query, [fragment_name])
    res = cur.fetchall()
    for val in res:
        avg_from_all_sites = val[0]
        break
    cur.close()
    return avg_from_all_sites or 0


def fetch_all_remote_sites(query, fragment_name, param_site_name):
    cur = conn.cursor()
    cur.execute(query, [fragment_name, param_site_name,param_site_name])
    res = cur.fetchall()
    list_res = []
    for val in res:
        list_res.append(val[0])
    cur.close()
    return list_res


def fetch_avg_vol_from_remote_site(query, fragment_name, remote_site_name):
    cur = conn.cursor()
    cur.execute(query, [fragment_name, remote_site_name])
    res = cur.fetchall()
    for val in res:
        avg_for_remote_site = val[0]
        break
    cur.close()
    return avg_for_remote_site or 0


def fetch_avg_write_vol_for_site(query, fragment_name, elgbl_site_name, write_op):
    cur = conn.cursor()
    cur.execute(query, [fragment_name, elgbl_site_name, write_op])
    res = cur.fetchall()
    for val in res:
        avg_for_write_elg_site = val[0]
        break
    cur.close()
    return avg_for_write_elg_site or 0


def fragment_reallocation_main(site_fragment_static_dict):
    site_fragment_realloc_dict = {}

    for site_name, fragment_list in site_fragment_static_dict.items():
        print("\n\n------------------------------------------")
        print("For Site: " + site_name)
        access_threshold_site=fetch_access_threeshold_per_site(site_name)
        print("Access Threshold defined for site "+site_name+" = "+str(access_threshold_site))
        for fragment_f in fragment_list:
            print("\nFor Fragment: "+fragment_f)
            table_name = 'log_info_' + site_name.lower()
            eligible_sites = []  # List of all eligible sites for reallocation

            # find average volume of read and write between fragment F and all sites
            avg_vol_from_all_sites_query = ('select avg(DataVol) from ' + table_name + ' where AFID = :fragment_f ')
            avg_vol_from_all_sites = fetch_avg_vol_from_all_sites(avg_vol_from_all_sites_query, fragment_f)
            print("\n\nAverage volume of read and write data between fragment " + fragment_f + " and all the sites = " + str(
                    avg_vol_from_all_sites))

            # For each remote site s
            list_remote_site_query = (
                    'select ASID, count(*) from ' + table_name + ' where AFID= :fragment_f and ASID != '
                                                                 ':site_name group by ASID having count(*) >= (select '
                                                                 'distinct(access_threeshold) from '
                                                                 'fragment_alloc_site_mapping where site_name = '
                                                                 ':site_name)')
            remote_site_list = fetch_all_remote_sites(list_remote_site_query, fragment_f, site_name)
            print("Remote Site list that satisfies access threshold: " + str(remote_site_list))

            # For each remote site loop and find average volume between two sites and fragment F

            for remote_site in remote_site_list:
                remote_site_avg_query = (
                        'select avg(DataVol) from ' + table_name + ' where AFID= :fragment_f and ASID= :remote_site')
                avg_vol_for_remote_site_s = fetch_avg_vol_from_remote_site(remote_site_avg_query, fragment_f,
                                                                           remote_site)
                print("Average volume of read and write data transmitted of remote site: " + remote_site + " = " + str(
                    avg_vol_for_remote_site_s))
                if avg_vol_for_remote_site_s > avg_vol_from_all_sites:
                    eligible_sites.append(remote_site)

            print("Remote sites which are eligible for fragment allocation:" + str(eligible_sites))
            if len(eligible_sites) == 1:
                site_fragment_realloc_dict[fragment_f] = eligible_sites[0]
                print("\n ----***----Allocate Fragment " + fragment_f + " at site " + eligible_sites[0]+"----***--\n")
            elif len(eligible_sites) > 1:
                max_write_count = 0
                max_write_elgbl_site = ''
                print("More than one Eligible sites found. Calculate volume for write data for each site.")
                for elgbl_site in eligible_sites:
                    avg_write_vol_for_site=0
                    avg_write_vol_for_site_query = (
                            'select avg(DataVol) from ' + table_name + ' where AFID= :fragment_f and ASID= :elgbl_site '
                                                                       ' and RorWAS= :write_oper')
                    avg_write_vol_for_site = fetch_avg_write_vol_for_site(avg_write_vol_for_site_query, fragment_f,
                                                                          elgbl_site, 'w')
                    print("Average Write Volume for Site "+elgbl_site+" = "+str(avg_write_vol_for_site))
                    if avg_write_vol_for_site > max_write_count:
                        max_write_count = avg_write_vol_for_site
                        max_write_elgbl_site = elgbl_site

                print("\n ----***----Allocate Fragment " + fragment_f + " at site " + max_write_elgbl_site
                      + "-------***-----  \n ")
                site_fragment_realloc_dict[fragment_f] = max_write_elgbl_site
    return site_fragment_realloc_dict


def update_fragment_reallocation(dictonary):
    for fragment, site in dictonary.items():
        realloc_update_query = 'update fragment_alloc_site_mapping set site_name= :site where fragment_name= :fragment'
        cur = conn.cursor()
        cur.execute(realloc_update_query, [site, fragment])
        res = conn.commit()
        cur.close()
