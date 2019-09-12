"""
This file contains all the functions that apply dictionaries or map know values
based on a direct relationship between variables. 
It currently contains:
    
    General Scripts: 
        pawp
        cities
        assign_basin
        
    Specific Scripts: 
        apply_dictionaries_wwtp
""" 
import pandas as pd
import numpy as np
from time import sleep
import os.path
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from geopy.exc import GeocoderServiceError
import fiona
import shapely
import shapely.geometry
from alphabet_detector import AlphabetDetector

print("HI PROJECT!")

# FILES #
citiesDictionary_file = "Dictionaries/Cities.csv"
wwtp_dictionary_file = "Sectors/MWWTP/Sources/WWTP_Dictionaries.xlsx"
industrial_wwtp_list = ["duplicate", "commercial", "Landfill", "landfill", "gold mine", "mine", "pulp", "compost", "stormwater", "leachate"]
lf_dict_file = "Sectors/Landfills/Sources/LF_Dictionaries.xlsx"
pawp_file = "Dictionaries/PAWP_Classes.xlsx"
basin_file = r"Dictionaries\BCWatersheds_Aug19\BCWatersheds_Aug19.shp"
basin_dict_file = "Dictionaries/Basins.csv"

###############################################################################
# Assign PAWP classes to pollutants from various sources using the consolidated
# dictionary
def pawp(df, path, parameterName="ParameterName", pawp_file=pawp_file, p=True):
    if p: print("\t\tIn Dictionaries: pawp...")
    pawp_file = "{}/{}".format(path, pawp_file)
    pawp_xls = pd.ExcelFile(pawp_file)
    pawp_dict = pd.read_excel(pawp_xls, sheet_name="Consolidated_PAWPs").set_index("Parameter").to_dict()
    
    df["PAWP_class"] = df[parameterName].map(pawp_dict["PAWP Class"])
    
    return df

###############################################################################
# These two functions use the geopy geocoders libraries (which uses Nomatim) to 
# ask a server the address of a lat lon. It then extracts relevant information 
# from the address and stashes it in __________ so we do not always have to ask 
# the server. This service is not designed for large batch runs so we must 
# cache our results and only use nomatim to fill in what we still don't know.
# Most likely, there will be a more accurate way to assign cities in the GIS 
# part, these city assignments will only be used for calcualations to estimate
# flows based on municipal capita.  

# A helper function for cities.
def assignLocation(df, torun, lat_label, lon_label, p=True):
    if p: print("\t\t\tIn Dictionaries: assignLocation...")
    ad = AlphabetDetector()
    timedOut = []
    done = True
    for row in torun:
        # Only fill in missing cities. 
        if pd.isnull(df.loc[row, "Province"]):
            # Don't run with missing lat lons. 
            if not any([pd.isnull(df.loc[row, lat_label]), pd.isnull(df.loc[row, lon_label])]): 
                try:
                    geolocator = Nominatim(timeout=10, user_agent="Single_Batch_Run")

                    if p: print("\t\t\t\tCities {}% complete...".format(int(row/len(df)*100)))
                    location = geolocator.reverse("{}, {}".format(df.loc[row, lat_label], df.loc[row, lon_label]))

                    if "address" in location.raw:
                        dictionary = location.raw['address']
                        # Can include Region, Province and Country if desired. 
                        if ~pd.isnull(df.loc[row, "Region"]):
                            if "hamlet" in dictionary:
                                if ad.only_alphabet_chars(dictionary["hamlet"], "LATIN"):
                                    df.loc[row, "Region"] = dictionary["hamlet"]
                                
                            elif "state_district" in dictionary:
                                if ad.only_alphabet_chars(dictionary["state_district"], "LATIN"):
                                    df.loc[row, "Region"] = dictionary["state_district"]
                                
                            elif "county" in dictionary:
                                if ad.only_alphabet_chars(dictionary["county"], "LATIN"):
                                    df.loc[row, "Region"] = dictionary["county"]
                                
                        else:
                            df.loc[row, "Region"] = np.NaN
                            
                        if "Province" in dictionary:
                            if ad.only_alphabet_chars(dictionary["state"], "LATIN"):
                                df.loc[row, "Province"] = dictionary["state"]
                            
                        if ad.only_alphabet_chars(dictionary["country"], "LATIN"):    
                            df.loc[row, "Country"] = dictionary["country"]
                        
                        if ~pd.isnull(df.loc[row, "City"]):
                            if "city" in dictionary:
                                if ad.only_alphabet_chars(dictionary["city"], "LATIN"):
                                    df.loc[row, "City"] = dictionary["city"]
                                
                            elif "town" in dictionary:
                                if ad.only_alphabet_chars(dictionary["town"], "LATIN"):
                                    df.loc[row, "City"] = dictionary["town"]
                                
                            elif "village" in dictionary:
                                if ad.only_alphabet_chars(dictionary["village"], "LATIN"):
                                    df.loc[row, "City"] = dictionary["village"]
                           
                        else:
                            df.loc[row, "City"] = np.NaN
                        
 
                        sleep(0.5) # in seconds
                except GeocoderTimedOut:
                    if p: print("\t\tTimed Out")
                    done = False
                    timedOut.append(row)
                except GeocoderServiceError:
                    if p: print("GeocoderServiceError! (Probably certificate)")
                    done = False
                    timedOut.append(row)
    return df, done, timedOut

# Main function that gets called from outside. Uses cache to assign the cities
# we have already searched as not to overload the Nomatim server. 
            # Only fill in missing cities. If unsure of the accuracy, don't put a
        #city in to df before running this function. 
def cities(df, path, lat_label="Latitude", lon_label="Longitude", citiesDictionary_file=citiesDictionary_file, keep_all=False, p=True):
    if p: print("\t\tIn Dictionaries: cities...")
    done = False
    # maxPass is the max number of times to ask the server about 1 location if
    #it fails. This avoid an eternal loop if there is no information or we get
    # blocked from the server. 
    maxPass = 5 
    passes = 0
    citiesDictionary = "{}/{}".format(path, citiesDictionary_file)
    df[lon_label] = df[lon_label].astype(float)
    df[lat_label] = df[lat_label].astype(float)
    
    # Convert to to Longitude West (as used by geopy)
    Longitude_west = df[lon_label] > 0
    df.loc[Longitude_west,lon_label] = df.loc[Longitude_west,lon_label] * -1

    # Make dictionary of those already known and apply it first
    if os.path.isfile(citiesDictionary):
        cities = pd.read_csv(citiesDictionary, dtype=str, encoding = 'ISO-8859-1')
        cities_copy = cities.copy()
        cities["tuple"] = list(zip(cities[lat_label].astype(float), cities[lon_label].astype(float)))
        cities.set_index("tuple", inplace=True)
        cities_dict = cities.to_dict()
        # Apply the dict. This will make sure there is a column to assign
        # values to. 
        for col in ["City", "Region", "Province", "Country"]:
            if not (col in df.columns):
                df[col] = np.NaN
                
        df["tuple"] = list(zip(df["Latitude"], df["Longitude"]))
        df["City"] = df["tuple"].map(cities_dict["City"]).fillna(df["City"])
        df["Province"] = df["tuple"].map(cities_dict["Province"]).fillna(df["Province"])
        df["Region"] = df["tuple"].map(cities_dict["Region"]).fillna(df["Region"])
        df["Country"] = df["tuple"].map(cities_dict["Country"]).fillna(df["Country"])
        #df = df.drop(columns="tuple")

    # Repeat even if it times out until it finishes or surpasses maxPass.
    set_df = df.drop_duplicates(subset="tuple")
    set_df.reset_index(inplace=True)
    torun = range(len(set_df))
    while ~done: 
        set_df, done, timedOut = assignLocation(set_df, torun, lat_label, lon_label)
        if done:
            break
        else:
            print("Citites not done...")
            if passes >= maxPass:
                print("\tExceeded max tries")
                break
            passes += 1
            torun = timedOut
    # Drop blank lat longs or else errors bellow
    drop = set_df[set_df["Latitude"].isnull()].index
    set_df = set_df.drop(drop)
    # Apply new cities to df
    set_df_dict = set_df.set_index("tuple").to_dict()
    df["City"] = df["tuple"].map(set_df_dict["City"]).fillna(df["City"])
    df["Province"] = df["tuple"].map(set_df_dict["Province"]).fillna(df["Province"])
    df["Region"] = df["tuple"].map(set_df_dict["Region"]).fillna(df["Region"])
    df["Country"] = df["tuple"].map(set_df_dict["Country"]).fillna(df["Country"])
    df = df.drop(columns="tuple")

    # Expand the dictionary (only by those searched for this time, ie has a Country)
    towrite = df.loc[~pd.isnull(df["Country"]), ["City", "Region", "Province", "Country", "Latitude", "Longitude"]] # only add the values that had data assigned
    towrite = pd.concat([cities_copy, towrite], sort=False)
    check = towrite.copy()
    towrite.drop_duplicates(["Latitude", "Longitude"], inplace = True)
    
    towrite.to_csv(citiesDictionary, index=None, encoding = 'utf-8')

    if not keep_all: 
        df.drop(columns=["Province", "Region", "Country"], inplace=True)
    
    return df, passes >= maxPass, check

###############################################################################
# 
def assign_basin(df, path, lat_label="Latitude", lon_label="Longitude", basin_file=basin_file, basin_dict_file=basin_dict_file, p=True):
    if p: print("\t\tIn Dictionaries: assign_basin...")
    basin_file = "{}/{}".format(path, basin_file)
    basin_dict_file = "{}/{}".format(path, basin_dict_file)
    
    # Read in lat longs already assigned to a Basin apply it to the dataframe. 
    # This is considerably faster than reassigning each basin. 
    df[lon_label] = df[lon_label].astype(float)
    df[lat_label] = df[lat_label].astype(float)
    Longitude_west = df[lon_label] > 0
    df.loc[Longitude_west,lon_label] = df.loc[Longitude_west,lon_label] * -1
    df["tuple"] = list(zip(df[lat_label], df[lon_label]))
    # Make dictionary of those already known and apply it first
    if os.path.isfile(basin_dict_file):
        basins = pd.read_csv(basin_dict_file, dtype=str, encoding = 'ISO-8859-1').set_index("tuple")
        basins_dict = basins.to_dict()
        
        # Apply the dict.
        df["Basin"] = df["tuple"].map(basins_dict["City"]).fillna(df["City"])
    
    # Assign new basins
    # Read in the basin file
    all_shapes_raw = fiona.open(basin_file)
    # convert them all to shapes and put them in a list
    all_shapes = []
    for s in all_shapes_raw:
        shape = shapely.geometry.asShape(s['geometry'])
        all_shapes.append(shape)
    
    n = 0
    blank_id = df[df["Basin"].isnull()].index
    for df_id in blank_id:
        if p: print("\t\t\t", df_id, round(n/len(df.index)*100,2),"% complete")
        # Create a shapely point for the lat lon pair
        # Note, the basin file geometry is reversed and longitude is the first coordinate
        point = shapely.geometry.Point(df.loc[df_id, lon_label], df.loc[df_id, lat_label])
        
        i = 0
        for s in all_shapes: 
            if s.contains(point):
                df.loc[df_id, "Basin"] = all_shapes_raw[i]["properties"]["WSCSDA_EN"]
            i+=1
        n += 1
    
    # Update basin dictionary file
    all_basins = df.loc[blank_id, ["tuple", "Basin"]].copy().set_index("tuple") #pd.concat([basins, df.loc[blank_id, ["tuple", "Basin"]]], sort=False)
    all_basins.to_csv(basin_dict_file, mode='a')
    
    # Drop tuple column
    df = df.drop(columns="tuple")
    
    return df
    
"""    
###############################################################################
# Municipal Waste Water Treatment Plants (MWWTP) #
###############################################################################
"""  
###############################################################################
# Assign service type categories to wwtps to estimate their flows. Also assign
# statscan populations for municipal estimations. 
def apply_dictionaries_wwtp(flows, path, wwtp_dictionary_file=wwtp_dictionary_file, p=True):
    if p: print("\tIn Dictionaries: apply_dictionaries_wwtp...")
    wwtp_dictionary_file = "{}/{}".format(path, wwtp_dictionary_file)
    wwtp_dict_xls = pd.ExcelFile(wwtp_dictionary_file)

    # Make the specific dictionaries
    type_dict = pd.read_excel(wwtp_dict_xls, sheet_name="FacTypeDict").set_index("Service Type").to_dict()
    specific_flow_est_dict = pd.read_excel(wwtp_dict_xls, sheet_name="FacilityTypePopEstimate", skipfooter=8).set_index("Service Type").to_dict() # not sure how skip footer works, I guess it doesn't matter if it maps everything
    stats_pop_dict = pd.read_excel(wwtp_dict_xls, sheet_name="StatCan Pop Dict").set_index("Geographic name").to_dict()
    
    # Apply Categories (facility/service type) "Type Dict"
    flows["ServiceCategories"] = np.NaN
    flows["ServiceCategories"] = flows["SubType"].map(type_dict["Category"])
    
    # Apply duplicates over top
    flows["ServiceCategories"] = flows["SubType"].map(type_dict["Duplicate"]).fillna(flows["ServiceCategories"])
    
    # Drop Duplicates or industrial sectors
    flows.drop(flows[flows["ServiceCategories"] == "duplicate"].index, inplace=True)
    flows.drop(flows[flows["ServiceCategories"].isin(industrial_wwtp_list)].index, inplace=True)

    # Apply populations from stats can. Will be used for unknown municipal flows
    flows["Population"] = flows["City"].map(stats_pop_dict[" Population, 2016 "])
    
    # Apply flows manually calculated for certain categories with units or 
    # capita specified. See the dictionary tab "FacilityTypePopEstimate" for 
    # details. 
    flows.loc[flows["Total Flow [m3/yr]"].isnull(), "Total Flow [m3/yr]"] = flows.loc[flows["Total Flow [m3/yr]"].isnull(),"SubType"].map(specific_flow_est_dict["m3/d"]).fillna(flows["Total Flow [m3/yr]"]) * 365 # [m3/d] * [d/yr] = [m3/yr]

    # Replace SubType iwth Service Categories for clarity (previous SubType, 
    # earlier known as ServiceType, is long-winded and/or nonsensical)
    flows["SubType"] = flows["ServiceCategories"].copy()
    flows.drop(columns="ServiceCategories", inplace=True)
    
    # Apply PAWP
    #flows = pawp(flows, path)
    
    # Apply Basins - long so comment out during rest of debugging
    flows = assign_basin(flows, path)

    return flows


"""    
###############################################################################
# Landfill (LF) #
###############################################################################
""" 

# ! no precip file
#popserved_dict - from monica_file
#statscan - until further review - from monica_file
#precipitation - needs to be updated
def lf_apply_dictionaries(flows, path, mon_xls, lf_dict_file=lf_dict_file, p=True):
    # Load files
    # Monica file mon_xls
    lf_dict_file = "{}/{}".format(path, lf_dict_file)
    lf_dict_xls = pd.ExcelFile(lf_dict_file)
    reclassify_tab = "reclassify"
    precipitation_tab = "precipitation"
    pop_served_tab = "PopServed Data"
    statscan_tab = "StatCanPopData"
    
#    # Make dictionaries
#    reclassify_dict = pd.read_excel(lf_dict_xls, sheet_name=reclassify_tab).set_index("Facility").to_dict()
#    precipitation_dict = pd.read_excel(lf_dict_xls, sheet_name=precipitation_tab).set_index("Basin").to_dict()
#    popserved_dict = pd.read_excel(mon_xls, sheet_name=pop_served_tab, skiprows=4).set_index("Site Name").to_dict()
#    statscan_dict = pd.read_excel(mon_xls, sheet_name=statscan_tab, skiprows=1).set_index("Geographic name").to_dict()
#    
#    # Recalssify, drop those no longer active landfills (ie - all), and remove the new column
#    flows["otherSectors"] = flows["Facility"].map(reclassify_dict["Potential Leachate Source"])
#    flows.drop(flows[~flows["otherSectors"].isnull()].index, inplace=True)
#    flows.drop(columns="otherSectors", inplace=True)
#    
#    # Map Population - estimate first, the overwrite with direct population 
#    # served (more precide)
#    flows["Population"]   = flows["City"].map(statscan_dict[' Population, 2016 ']).fillna(np.NaN)
#    flows["Population"]   = flows["Facility"].map(popserved_dict['Population']).fillna(flows["Population"])
#    
#    # Map MSW (Municipal Solid Waste) that we have
#    flows["MSW [t/yr]"] = flows["Facility"].map(popserved_dict['Annual Tonnage MSW [t/yr]']).fillna(flows["MSW [t/yr]"])
#    
    # Map Basins
    flows = assign_basin(flows, path, p=p)
        
    # Map precipitation to basin
    flows["Precipitation [mm]"]  = np.NaN#flows["Basin"].map(precipitation_dict[""])

    return flows




