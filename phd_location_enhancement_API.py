"""
PhD-Level Home/Road Quadrant Enhancement (API + Master Translation)
===================================================================
1. Fetches LIVE KenPom rankings.
2. Applies MASTER DICTIONARY to fix known naming issues.
3. Uses FUZZY MATCHING as a backup.
4. Generates validated Home/Road splits.
5. Prints a list of any REMAINING missing teams.
"""

import pandas as pd
import numpy as np
import requests
import os
import difflib

# ==============================================================================
# CONFIGURATION
# ==============================================================================
BOX_SCORE_FILE = "master_box_scores_2026.csv"
KP_API_KEY = "18ee6ae93a94ade96fc899611578ef42f57ad96e09845cb585ee3b2aab1444fa"
SEASON_YEAR = 2026

OUTPUT_HOME = "team_home_performance_VALIDATED_2026.csv"
OUTPUT_ROAD = "team_road_performance_VALIDATED_2026.csv"

# Statistical parameters
MIN_GAMES_HIGH_CONF = 5     
MIN_GAMES_MEDIUM_CONF = 3   
BAYESIAN_PRIOR_WEIGHT = 4.0 

# ==============================================================================
# MASTER TRANSLATION DICTIONARY (2025-26 SEASON)
# ==============================================================================

KENPOM_TRANSLATION = {

    # --- ACC ---
    "Boston College Eagles": "Boston College",
    "California Golden Bears": "California",
    "Clemson Tigers": "Clemson",
    "Duke Blue Devils": "Duke",
    "Florida State Seminoles": "Florida St.",
    "Georgia Tech Yellow Jackets": "Georgia Tech",
    "Louisville Cardinals": "Louisville",
    "Miami Hurricanes": "Miami FL",  # Distinguishes from Miami OH
    "NC State Wolfpack": "N.C. State",
    "North Carolina Tar Heels": "North Carolina",
    "Notre Dame Fighting Irish": "Notre Dame",
    "Pittsburgh Panthers": "Pittsburgh",
    "SMU Mustangs": "SMU",
    "Stanford Cardinal": "Stanford",
    "Syracuse Orange": "Syracuse",
    "Virginia Cavaliers": "Virginia",
    "Virginia Tech Hokies": "Virginia Tech",
    "Wake Forest Demon Deacons": "Wake Forest",

    # --- ATLANTIC 10 (A-10) ---
    "Davidson Wildcats": "Davidson",
    "Dayton Flyers": "Dayton",
    "Duquesne Dukes": "Duquesne",
    "Fordham Rams": "Fordham",
    "George Mason Patriots": "George Mason",
    "George Washington Revolutionaries": "George Washington",
    "George Washington Colonials": "George Washington",  # Legacy support
    "La Salle Explorers": "La Salle",
    "Loyola Chicago Ramblers": "Loyola Chicago",
    "Rhode Island Rams": "Rhode Island",
    "Richmond Spiders": "Richmond",
    "St. Bonaventure Bonnies": "St. Bonaventure",
    "Saint Bonaventure Bonnies": "St. Bonaventure",
    "Saint Joseph's Hawks": "Saint Joseph's",
    "Saint Louis Billikens": "Saint Louis",
    "VCU Rams": "VCU",

    # --- AMERICA EAST ---
    "Albany Great Danes": "Albany",
    "Binghamton Bearcats": "Binghamton",
    "Bryant Bulldogs": "Bryant",
    "Maine Black Bears": "Maine",
    "New Hampshire Wildcats": "New Hampshire",
    "NJIT Highlanders": "NJIT",
    "New Jersey Institute of Technology": "NJIT",
    "UAlbany Great Danes": "Albany",
    "UMass Lowell River Hawks": "UMass Lowell",
    "UMBC Retrievers": "UMBC",
    "University of Maryland Baltimore County": "UMBC",
    "Vermont Catamounts": "Vermont",

    # --- AMERICAN ATHLETIC CONFERENCE (AAC) ---
    "Charlotte 49ers": "Charlotte",
    "East Carolina Pirates": "East Carolina",
    "Florida Atlantic Owls": "Florida Atlantic",
    "Memphis Tigers": "Memphis",
    "North Texas Mean Green": "North Texas",
    "Rice Owls": "Rice",
    "South Florida Bulls": "South Florida",
    "Temple Owls": "Temple",
    "Tulane Green Wave": "Tulane",
    "Tulsa Golden Hurricane": "Tulsa",
    "UAB Blazers": "UAB",
    "USF Bulls": "South Florida",
    "UTSA Roadrunners": "UTSA",
    "Wichita State Shockers": "Wichita St.",

    # --- ASUN ---
    "Austin Peay Governors": "Austin Peay",
    "Bellarmine Knights": "Bellarmine",
    "Central Arkansas Bears": "Central Arkansas",
    "Eastern Kentucky Colonels": "EKU",
    "EKU Colonels": "EKU",
    "FGCU Eagles": "FGCU",
    "Florida Gulf Coast Eagles": "FGCU",
    "Florida Gulf Coast University": "FGCU",
    "Jacksonville Dolphins": "Jacksonville",
    "Lipscomb Bisons": "Lipscomb",
    "North Alabama Lions": "North Alabama",
    "North Florida Ospreys": "North Florida",
    "Queens University Royals": "Queens",
    "Queens Royals": "Queens",
    "Queens (NC) Royals": "Queens",
    "Stetson Hatters": "Stetson",
    "West Georgia Wolves": "West Georgia",

    # --- BIG 12 ---
    "Arizona Wildcats": "Arizona",
    "Arizona State Sun Devils": "Arizona St.",
    "Baylor Bears": "Baylor",
    "BYU Cougars": "BYU",
    "Brigham Young University Cougars": "BYU",
    "UCF Golden Knights": "UCF",
    "Central Florida Golden Knights": "UCF",
    "Cincinnati Bearcats": "Cincinnati",
    "Colorado Buffaloes": "Colorado",
    "Houston Cougars": "Houston",
    "Iowa State Cyclones": "Iowa St.",
    "Kansas Jayhawks": "Kansas",
    "Kansas State Wildcats": "Kansas St.",
    "Oklahoma State Cowboys": "Oklahoma St.",
    "TCU Horned Frogs": "TCU",
    "Texas Christian University Horned Frogs": "TCU",
    "Texas Tech Red Raiders": "Texas Tech",
    "Utah Utes": "Utah",
    "West Virginia Mountaineers": "West Virginia",

    # --- BIG EAST ---
    "Butler Bulldogs": "Butler",
    "Connecticut Huskies": "Connecticut",
    "UConn Huskies": "Connecticut",
    "UConn": "Connecticut",
    "Creighton Bluejays": "Creighton",
    "DePaul Blue Demons": "DePaul",
    "Georgetown Hoyas": "Georgetown",
    "Marquette Golden Eagles": "Marquette",
    "Providence Friars": "Providence",
    "Seton Hall Pirates": "Seton Hall",
    "St. John's Red Storm": "St. John's",
    "Saint John's Red Storm": "St. John's",
    "Villanova Wildcats": "Villanova",
    "Xavier Musketeers": "Xavier",

    # --- BIG SKY ---
    "Eastern Washington Eagles": "Eastern Washington",
    "Idaho Vandals": "Idaho",
    "Idaho State Bengals": "Idaho State",
    "Montana Grizzlies": "Montana",
    "Montana State Bobcats": "Montana State",
    "Northern Arizona Lumberjacks": "Northern Arizona",
    "Northern Colorado Bears": "Northern Colorado",
    "Portland State Vikings": "Portland State",
    "Sacramento State Hornets": "Sacramento St.",
    "Sac State Hornets": "Sacramento St.",
    "Weber State Wildcats": "Weber State",

    # --- BIG SOUTH ---
    "Charleston Southern Buccaneers": "Charleston Southern",
    "Gardner-Webb Runnin' Bulldogs": "Gardner Webb",
    "Gardner-Webb": "Gardner Webb",
    "High Point Panthers": "High Point",
    "Longwood Lancers": "Longwood",
    "Presbyterian Blue Hose": "Presbyterian",
    "Presbyterian College": "Presbyterian",
    "Radford Highlanders": "Radford",
    "UNC Asheville Bulldogs": "UNC Asheville",
    "University of North Carolina Asheville": "UNC Asheville",
    "USC Upstate Spartans": "USC Upstate",
    "South Carolina Upstate Spartans": "USC Upstate",
    "Winthrop Eagles": "Winthrop",

    # --- BIG TEN ---
    "Illinois Fighting Illini": "Illinois",
    "Indiana Hoosiers": "Indiana",
    "Iowa Hawkeyes": "Iowa",
    "Maryland Terrapins": "Maryland",
    "Michigan Wolverines": "Michigan",
    "Michigan State Spartans": "Michigan St.",
    "Minnesota Golden Gophers": "Minnesota",
    "Nebraska Cornhuskers": "Nebraska",
    "Northwestern Wildcats": "Northwestern",
    "Ohio State Buckeyes": "Ohio St.",
    "Oregon Ducks": "Oregon",
    "Penn State Nittany Lions": "Penn St.",
    "Purdue Boilermakers": "Purdue",
    "Rutgers Scarlet Knights": "Rutgers",
    "UCLA Bruins": "UCLA",
    "USC Trojans": "USC",
    "Washington Huskies": "Washington",
    "Wisconsin Badgers": "Wisconsin",

    # --- BIG WEST ---
    "Cal Poly Mustangs": "Cal Poly",
    "Cal State Bakersfield Roadrunners": "Cal St. Bakersfield",
    "CSU Bakersfield Roadrunners": "Cal St. Bakersfield",
    "Cal State Fullerton Titans": "Cal St. Fullerton",
    "CSU Fullerton Titans": "Cal St. Fullerton",
    "Cal State Northridge Matadors": "CSUN",
    "CSUN Matadors": "CSUN",
    "Hawaii Rainbow Warriors": "Hawaii",
    "Hawai'i Rainbow Warriors": "Hawaii",
    "Long Beach State Beach": "Long Beach St.",
    "UC Davis Aggies": "UC Davis",
    "UC Irvine Anteaters": "UC Irvine",
    "UC Riverside Highlanders": "UC Riverside",
    "UC San Diego Tritons": "UC San Diego",
    "UCSD Tritons": "UC San Diego",
    "UC Santa Barbara Gauchos": "UC Santa Barbara",
    "UCSB Gauchos": "UC Santa Barbara",

    # --- CAA (COASTAL ATHLETIC ASSOCIATION) ---
    "Campbell Fighting Camels": "Campbell",
    "Charleston Cougars": "Charleston",
    "College of Charleston Cougars": "Charleston",
    "Drexel Dragons": "Drexel",
    "Elon Phoenix": "Elon",
    "Hampton Pirates": "Hampton",
    "Hofstra Pride": "Hofstra",
    "Monmouth Hawks": "Monmouth",
    "North Carolina A&T Aggies": "N.C. A&T",
    "N.C. A&T Aggies": "N.C. A&T",
    "NC A&T Aggies": "N.C. A&T",
    "Northeastern Huskies": "Northeastern",
    "Stony Brook Seawolves": "Stony Brook",
    "Towson Tigers": "Towson",
    "UNC Wilmington Seahawks": "UNCW",
    "UNCW Seahawks": "UNCW",
    "William & Mary Tribe": "William & Mary",
    "College of William & Mary Tribe": "William & Mary",

    # --- CONFERENCE USA (CUSA) ---
    "Delaware Blue Hens": "Delaware",
    "FIU Panthers": "FIU",
    "Florida International Panthers": "FIU",
    "Jacksonville State Gamecocks": "Jacksonville St.",
    "Jax State Gamecocks": "Jacksonville St.",
    "Kennesaw State Owls": "Kennesaw St.",
    "Liberty Flames": "Liberty",
    "Louisiana Tech Bulldogs": "Louisiana Tech",
    "LA Tech Bulldogs": "Louisiana Tech",
    "Middle Tennessee Blue Raiders": "Middle Tennessee",
    "MTSU Blue Raiders": "Middle Tennessee",
    "Missouri State Bears": "Missouri St.",
    "New Mexico State Aggies": "New Mexico St.",
    "NM State Aggies": "New Mexico St.",
    "Sam Houston Bearkats": "Sam Houston St.",
    "Sam Houston State Bearkats": "Sam Houston St.",
    "UTEP Miners": "UTEP",
    "WKU Hilltoppers": "Western Kentucky",
    "Western Kentucky Hilltoppers": "Western Kentucky",

    # --- HORIZON LEAGUE ---
    "Cleveland State Vikings": "Cleveland St.",
    "Detroit Mercy Titans": "Detroit Mercy",
    "Detroit Titans": "Detroit Mercy",
    "Green Bay Phoenix": "Green Bay",
    "IU Indianapolis Jaguars": "IU Indy",
    "IU Indy Jaguars": "IU Indy",
    "IUPUI Jaguars": "IU Indy",  # Legacy support
    "Milwaukee Panthers": "Milwaukee",
    "Northern Kentucky Norse": "Northern Kentucky",
    "NKU Norse": "Northern Kentucky",
    "Oakland Golden Grizzlies": "Oakland",
    "Purdue Fort Wayne Mastodons": "Purdue Fort Wayne",
    "PFW Mastodons": "Purdue Fort Wayne",
    "Robert Morris Colonials": "Robert Morris",
    "RMU Colonials": "Robert Morris",
    "Wright State Raiders": "Wright State",
    "Youngstown State Penguins": "Youngstown St.",

    # --- IVY LEAGUE ---
    "Brown Bears": "Brown",
    "Columbia Lions": "Columbia",
    "Cornell Big Red": "Cornell",
    "Dartmouth Big Green": "Dartmouth",
    "Harvard Crimson": "Harvard",
    "Penn Quakers": "Pennsylvania",
    "University of Pennsylvania Quakers": "Pennsylvania",
    "Princeton Tigers": "Princeton",
    "Yale Bulldogs": "Yale",

    # --- MAAC ---
    "Canisius Golden Griffins": "Canisius",
    "Canisius University Golden Griffins": "Canisius",
    "Fairfield Stags": "Fairfield",
    "Iona Gaels": "Iona",
    "Iona University Gaels": "Iona",
    "Manhattan Jaspers": "Manhattan",
    "Manhattan University Jaspers": "Manhattan",
    "Marist Red Foxes": "Marist",
    "Merrimack Warriors": "Merrimack",
    "Merrimack College Warriors": "Merrimack",
    "Mount St. Mary's Mountaineers": "Mount St. Mary's",
    "Mount Saint Mary's Mountaineers": "Mount St. Mary's",
    "Niagara Purple Eagles": "Niagara",
    "Quinnipiac Bobcats": "Quinnipiac",
    "Rider Broncs": "Rider",
    "Sacred Heart Pioneers": "Sacred Heart",
    "Saint Peter's Peacocks": "Saint Peter's",
    "St. Peter's Peacocks": "Saint Peter's",
    "Siena Saints": "Siena",

    # --- MAC (MID-AMERICAN) ---
    "Akron Zips": "Akron",
    "Ball State Cardinals": "Ball St.",
    "Bowling Green Falcons": "Bowling Green",
    "Buffalo Bulls": "Buffalo",
    "Central Michigan Chippewas": "Central Michigan",
    "Eastern Michigan Eagles": "Eastern Michigan",
    "Kent State Golden Flashes": "Kent St.",
    "Massachusetts Minutemen": "Massachusetts",
    "UMass Minutemen": "Massachusetts",
    "Miami (OH) RedHawks": "Miami OH",
    "Miami RedHawks": "Miami OH",
    "Northern Illinois Huskies": "Northern Illinois",
    "Ohio Bobcats": "Ohio",
    "Toledo Rockets": "Toledo",
    "Western Michigan Broncos": "Western Michigan",

    # --- MEAC ---
    "Coppin State Eagles": "Coppin St.",
    "Delaware State Hornets": "Delaware St.",
    "Howard Bison": "Howard",
    "Maryland Eastern Shore Hawks": "Maryland Eastern Shore",
    "Maryland-Eastern Shore Hawks": "Maryland Eastern Shore",
    "UMES Hawks": "Maryland Eastern Shore",
    "Morgan State Bears": "Morgan St.",
    "Norfolk State Spartans": "Norfolk St.",
    "North Carolina Central Eagles": "North Carolina Central",
    "NC Central Eagles": "North Carolina Central",
    "N.C. Central Eagles": "North Carolina Central",
    "South Carolina State Bulldogs": "S.C. State",
    "S.C. State Bulldogs": "S.C. State",
    "SC State Bulldogs": "S.C. State",

    # --- MOUNTAIN WEST ---
    "Air Force Falcons": "Air Force",
    "Boise State Broncos": "Boise St.",
    "Colorado State Rams": "Colorado St.",
    "Fresno State Bulldogs": "Fresno St.",
    "Grand Canyon Antelopes": "Grand Canyon",
    "GCU Antelopes": "Grand Canyon",
    "Nevada Wolf Pack": "Nevada",
    "New Mexico Lobos": "New Mexico",
    "San Diego State Aztecs": "San Diego St.",
    "SDSU Aztecs": "San Diego St.",
    "San José State Spartans": "San Jose St.",
    "San Jose State Spartans": "San Jose St.",
    "UNLV Runnin' Rebels": "UNLV",
    "Nevada-Las Vegas Runnin' Rebels": "UNLV",
    "Utah State Aggies": "Utah St.",
    "Wyoming Cowboys": "Wyoming",

    # --- MISSOURI VALLEY (MVC) ---
    "Belmont Bruins": "Belmont",
    "Bradley Braves": "Bradley",
    "Drake Bulldogs": "Drake",
    "Evansville Purple Aces": "Evansville",
    "UIC Flames": "UIC",
    "Illinois-Chicago Flames": "UIC",
    "Illinois State Redbirds": "Illinois St.",
    "Indiana State Sycamores": "Indiana St.",
    "Murray State Racers": "Murray St.",
    "Northern Iowa Panthers": "UNI",
    "UNI Panthers": "UNI",
    "Southern Illinois Salukis": "Southern Illinois",
    "Valparaiso Beacons": "Valparaiso",
    "Valpo Beacons": "Valparaiso",

    # --- NEC (NORTHEAST) ---
    "Central Connecticut State Blue Devils": "Central Connecticut",
    "CCSU Blue Devils": "Central Connecticut",
    "Chicago State Cougars": "Chicago St.",
    "Fairleigh Dickinson Knights": "Fairleigh Dickinson",
    "FDU Knights": "Fairleigh Dickinson",
    "Le Moyne Dolphins": "Le Moyne",
    "LIU Sharks": "Long Island",
    "Long Island University Sharks": "Long Island",
    "Long Island Sharks": "Long Island",
    "Mercyhurst Lakers": "Mercyhurst",
    "New Haven Chargers": "New Haven",
    "Saint Francis Red Flash": "St. Francis (PA)",
    "Saint Francis (PA) Red Flash": "St. Francis (PA)",
    "St. Francis (PA) Red Flash": "St. Francis (PA)",
    "Stonehill Skyhawks": "Stonehill",
    "Wagner Seahawks": "Wagner",

    # --- OHIO VALLEY (OVC) ---
    "Eastern Illinois Panthers": "Eastern Illinois",
    "Lindenwood Lions": "Lindenwood",
    "Little Rock Trojans": "Little Rock",
    "Morehead State Eagles": "Morehead St.",
    "Southeast Missouri State Redhawks": "Southeast Missouri",
    "SEMO Redhawks": "Southeast Missouri",
    "SIU Edwardsville Cougars": "SIUE",
    "SIUE Cougars": "SIUE",
    "Southern Indiana Screaming Eagles": "Southern Indiana",
    "Tennessee State Tigers": "Tennessee St.",
    "Tennessee Tech Golden Eagles": "Tennessee Tech",
    "UT Martin Skyhawks": "UT Martin",
    "Tennessee-Martin Skyhawks": "UT Martin",
    "Western Illinois Leathernecks": "Western Illinois",

    # --- PATRIOT LEAGUE ---
    "American University Eagles": "American",
    "American Eagles": "American",
    "Army West Point Black Knights": "Army West Point",
    "Army Black Knights": "Army West Point",
    "Boston University Terriers": "Boston University",
    "Boston U Terriers": "Boston University",
    "Bucknell Bison": "Bucknell",
    "Colgate Raiders": "Colgate",
    "Holy Cross Crusaders": "Holy Cross",
    "Lafayette Leopards": "Lafayette",
    "Lehigh Mountain Hawks": "Lehigh",
    "Loyola Maryland Greyhounds": "Loyola MD",
    "Loyola (MD) Greyhounds": "Loyola MD",
    "Loyola MD Greyhounds": "Loyola MD",
    "Navy Midshipmen": "Navy",

    # --- SEC ---
    "Alabama Crimson Tide": "Alabama",
    "Arkansas Razorbacks": "Arkansas",
    "Auburn Tigers": "Auburn",
    "Florida Gators": "Florida",
    "Georgia Bulldogs": "Georgia",
    "Kentucky Wildcats": "Kentucky",
    "LSU Tigers": "LSU",
    "Louisiana State University Tigers": "LSU",
    "Mississippi Rebels": "Ole Miss",
    "Ole Miss Rebels": "Ole Miss",
    "Mississippi State Bulldogs": "Mississippi St.",
    "Missouri Tigers": "Missouri",
    "Mizzou Tigers": "Missouri",
    "Oklahoma Sooners": "Oklahoma",
    "South Carolina Gamecocks": "South Carolina",
    "Tennessee Volunteers": "Tennessee",
    "Texas Longhorns": "Texas",
    "Texas A&M Aggies": "Texas A&M",
    "Vanderbilt Commodores": "Vanderbilt",

    # --- SOUTHERN (SoCon) ---
    "Chattanooga Mocs": "Chattanooga",
    "University of Tennessee at Chattanooga": "Chattanooga",
    "The Citadel Bulldogs": "The Citadel",
    "East Tennessee State Buccaneers": "ETSU",
    "ETSU Buccaneers": "ETSU",
    "Furman Paladins": "Furman",
    "Mercer Bears": "Mercer",
    "Samford Bulldogs": "Samford",
    "UNC Greensboro Spartans": "UNC Greensboro",
    "UNCG Spartans": "UNC Greensboro",
    "VMI Keydets": "VMI",
    "Virginia Military Institute Keydets": "VMI",
    "Western Carolina Catamounts": "Western Carolina",
    "Wofford Terriers": "Wofford",

    # --- SOUTHLAND ---
    "East Texas A&M Lions": "East Texas A&M",
    "Texas A&M-Commerce Lions": "East Texas A&M",  # Legacy support
    "Houston Christian Huskies": "HCU",
    "HCU Huskies": "HCU",
    "Incarnate Word Cardinals": "UIW",
    "UIW Cardinals": "UIW",
    "Lamar Cardinals": "Lamar",
    "McNeese Cowboys": "McNeese",
    "McNeese State Cowboys": "McNeese",
    "New Orleans Privateers": "New Orleans",
    "Nicholls Colonels": "Nicholls",
    "Nicholls State Colonels": "Nicholls",
    "Northwestern State Demons": "Northwestern St.",
    "Southeastern Louisiana Lions": "Southeastern Louisiana",
    "Southeastern Lions": "Southeastern Louisiana",
    "Stephen F. Austin Lumberjacks": "Stephen F. Austin",
    "SFA Lumberjacks": "Stephen F. Austin",
    "Texas A&M-Corpus Christi Islanders": "Texas A&M Corpus Chris",
    "A&M-Corpus Christi Islanders": "Texas A&M Corpus Chris",
    "UTRGV Vaqueros": "UTRGV",
    "UT Rio Grande Valley Vaqueros": "UTRGV",

    # --- SUMMIT LEAGUE ---
    "Denver Pioneers": "Denver",
    "Kansas City Roos": "Kansas City",
    "UMKC Roos": "Kansas City",
    "North Dakota Fighting Hawks": "North Dakota",
    "North Dakota State Bison": "North Dakota St.",
    "Omaha Mavericks": "Omaha",
    "Nebraska Omaha Mavericks": "Omaha",
    "Oral Roberts Golden Eagles": "Oral Roberts",
    "South Dakota Coyotes": "South Dakota",
    "South Dakota State Jackrabbits": "South Dakota St.",
    "St. Thomas-Minnesota Tommies": "St. Thomas",
    "Saint Thomas Tommies": "St. Thomas",
    "St. Thomas (MN) Tommies": "St. Thomas",

    # --- SUN BELT ---
    "App State Mountaineers": "Appalachian St.",
    "Appalachian State Mountaineers": "Appalachian St.",
    "Arkansas State Red Wolves": "Arkansas St.",
    "Coastal Carolina Chanticleers": "Coastal Carolina",
    "Georgia Southern Eagles": "Georgia Southern",
    "Georgia State Panthers": "Georgia St.",
    "James Madison Dukes": "James Madison",
    "JMU Dukes": "James Madison",
    "Louisiana Ragin' Cajuns": "Louisiana",
    "Louisiana-Lafayette Ragin' Cajuns": "Louisiana",
    "UL Monroe Warhawks": "ULM",
    "ULM Warhawks": "ULM",
    "Louisiana-Monroe Warhawks": "ULM",
    "Marshall Thundering Herd": "Marshall",
    "Old Dominion Monarchs": "Old Dominion",
    "South Alabama Jaguars": "South Alabama",
    "Southern Miss Golden Eagles": "Southern Miss",
    "Texas State Bobcats": "Texas St.",
    "Troy Trojans": "Troy",

    # --- SWAC ---
    "Alabama A&M Bulldogs": "Alabama A&M",
    "Alabama State Hornets": "Alabama St.",
    "Alcorn State Braves": "Alcorn St.",
    "Arkansas-Pine Bluff Golden Lions": "Arkansas Pine Bluff",
    "UAPB Golden Lions": "Arkansas Pine Bluff",
    "Bethune-Cookman Wildcats": "Bethune Cookman",
    "Florida A&M Rattlers": "Florida A&M",
    "FAMU Rattlers": "Florida A&M",
    "Grambling State Tigers": "Grambling St.",
    "Grambling Tigers": "Grambling St.",
    "Jackson State Tigers": "Jackson St.",
    "Mississippi Valley State Delta Devils": "Mississippi Valley St.",
    "MVSU Delta Devils": "Mississippi Valley St.",
    "Prairie View A&M Panthers": "Prairie View A&M",
    "PVAMU Panthers": "Prairie View A&M",
    "Southern Jaguars": "Southern",
    "Southern University Jaguars": "Southern",
    "Texas Southern Tigers": "Texas Southern",

    # --- WAC ---
    "Abilene Christian Wildcats": "Abilene Christian",
    "ACU Wildcats": "Abilene Christian",
    "California Baptist Lancers": "Cal Baptist",
    "CBU Lancers": "Cal Baptist",
    "Southern Utah Thunderbirds": "Southern Utah",
    "Tarleton State Texans": "Tarleton St.",
    "Tarleton Texans": "Tarleton St.",
    "UT Arlington Mavericks": "UT Arlington",
    "Texas-Arlington Mavericks": "UT Arlington",
    "Utah Tech Trailblazers": "Utah Tech",
    "Utah Valley Wolverines": "Utah Valley",
    "UVU Wolverines": "Utah Valley",

    # --- WEST COAST CONFERENCE (WCC) ---
    "Gonzaga Bulldogs": "Gonzaga",
    "Loyola Marymount Lions": "LMU",
    "LMU Lions": "LMU",
    "Oregon State Beavers": "Oregon St.",
    "Pacific Tigers": "Pacific",
    "Pepperdine Waves": "Pepperdine",
    "Portland Pilots": "Portland",
    "Saint Mary's Gaels": "Saint Mary's",
    "St. Mary's Gaels": "Saint Mary's",
    "San Diego Toreros": "San Diego",
    "San Francisco Dons": "San Francisco",
    "Santa Clara Broncos": "Santa Clara",
    "Seattle U Redhawks": "Seattle",
    "Seattle Redhawks": "Seattle",
    "Washington State Cougars": "Washington St.",
    "Wazzu Cougars": "Washington St.",


    # --- Fixes for Teams in your "Missing Report" ---
    "Army": "Army West Point",
    "East Tennessee St.": "ETSU",
    "Eastern Kentucky": "EKU",
    "Florida Gulf Coast": "FGCU",
    "Idaho St.": "Idaho State",       # Box score uses abbr., KenPom uses full
    "Illinois Chicago": "UIC",
    "Incarnate Word": "UIW",
    "LIU": "Long Island",             # KenPom uses "Long Island", Box Score uses "LIU"
    "Loyola Marymount": "LMU",
    "Mississippi": "Ole Miss",        # "Mississippi" is the official name for Ole Miss
    "Saint Francis": "St. Francis (PA)",
    "St. Francis (PA)": "St. Francis (PA)",
    
    # --- Fixes for "Bad Fuzzy Matches" seen in your log ---
    "SE Louisiana": "Southeastern Louisiana",       # Was matching to "Louisiana Monroe"
    "SE Louisiana Lions": "Southeastern Louisiana",
    "Southwestern Christian": None,                 # Non-D1 team (Prevent bad match)
    "Southern Wesleyan": None,                      # Non-D1 team (Prevent bad match)
    "Washington Adventist": None,                   # Non-D1 team (Prevent bad match)
    
    # --- Common "Hidden" Misses ---
    "UMKC": "Kansas City",
    "Gardner-Webb": "Gardner Webb",   # Hyphen removal
    "Maryland Eastern Shore": "Maryland Eastern Shore", # Ensure exact string match
    "St. Thomas (MN)": "St. Thomas",
    "Texas A&M-Corpus Christi": "Texas A&M Corpus Chris"
}



# ==============================================================================
# HELPER: SMART NAME MATCHING
# ==============================================================================
def create_smart_rank_map(box_names, kp_names, kp_ranks):
    """
    Maps box score names to KenPom ranks using Dictionary -> Exact -> Fuzzy.
    """
    print("\n🔗 SYNCHRONIZING TEAM NAMES...")
    
    # Normalize KenPom names for easier matching
    kp_clean_map = {name.lower().strip(): name for name in kp_names}
    kp_lookup = dict(zip(kp_names, kp_ranks))
    
    final_map = {}
    matched_count = 0
    dict_match_count = 0
    fuzzy_count = 0
    
    # Track which KenPom teams are found
    found_kp_teams = set()
    
    for name in box_names:
        clean_name = str(name).strip()
        lower_name = clean_name.lower()
        
        target_kp_name = None
        
        # 1. MASTER DICTIONARY CHECK (Priority)
        if clean_name in KENPOM_TRANSLATION:
            target_kp_name = KENPOM_TRANSLATION[clean_name]
            match_type = "Dictionary"

        # 2. Exact Match (If not found in dict)
        if not target_kp_name:
            if clean_name in kp_lookup:
                target_kp_name = clean_name
                match_type = "Exact"
            elif lower_name in kp_clean_map:
                target_kp_name = kp_clean_map[lower_name]
                match_type = "Exact (Case)"

        # 3. Fuzzy Match (Fallback)
        if not target_kp_name:
            matches = difflib.get_close_matches(clean_name, kp_names, n=1, cutoff=0.6)
            if matches:
                target_kp_name = matches[0]
                match_type = "Fuzzy"

        # 4. Save Match
        if target_kp_name:
            if target_kp_name in kp_lookup:
                final_map[clean_name] = kp_lookup[target_kp_name]
                found_kp_teams.add(target_kp_name)
                matched_count += 1
                if match_type == "Dictionary": dict_match_count += 1
                elif match_type == "Fuzzy": 
                    fuzzy_count += 1
                    if fuzzy_count <= 5:
                        print(f"   ✨ Fuzzy Match: '{clean_name}' ➡️ '{target_kp_name}'")
    
    print(f"   ✅ Total Matched: {matched_count}")
    print(f"      - Dictionary Fixes: {dict_match_count}")
    print(f"      - Fuzzy Matches: {fuzzy_count}")
    
    # Report Missing Teams
    all_kp_set = set(kp_names)
    missing = sorted(list(all_kp_set - found_kp_teams))
    print(f"\n   ⚠️  MISSING TEAMS REPORT ({len(missing)} Teams Unmatched):")
    if len(missing) > 0:
        print(f"   {', '.join(missing[:10])} ...")
        print("   (Check these names in your Box Score CSV to add to Dictionary)")

    return final_map

# ==============================================================================
# MAIN DATA GENERATION
# ==============================================================================
def generate_validated_home_road_data():
    print("\n" + "="*70)
    print("🎓 GENERATING PhD-LEVEL HOME/ROAD DATA (FINAL VALIDATION)")
    print("="*70)
    
    # 1. Load Box Scores
    if not os.path.exists(BOX_SCORE_FILE):
        print(f"❌ {BOX_SCORE_FILE} not found")
        return
    
    box_df = pd.read_csv(BOX_SCORE_FILE)
    print(f"   📖 Loaded {len(box_df)} box score records.")
    
    # 2. Get Live KenPom Data
    print(f"   ☁️  Connecting to KenPom API (Year: {SEASON_YEAR})...")
    
    url = f"https://kenpom.com/api.php?endpoint=ratings&y={SEASON_YEAR}"
    headers = {
        "Authorization": f"Bearer {KP_API_KEY}",
        "Accept": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        kp_df = pd.DataFrame(data)
        
        if 'TeamName' not in kp_df.columns: kp_df.rename(columns={'Team': 'TeamName'}, inplace=True)
        kp_df['AdjEM'] = pd.to_numeric(kp_df['AdjEM'])
        kp_df = kp_df.sort_values(by='AdjEM', ascending=False).reset_index(drop=True)
        kp_df['Rank'] = kp_df.index + 1
        print(f"      ✅ API Success: Loaded {len(kp_df)} official rankings.")
        
    except Exception as e:
        print(f"      ❌ API Error: {e}")
        return

    # 3. Create Smart Rank Map
    unique_opponents = box_df['Opponent'].unique()
    rank_map = create_smart_rank_map(unique_opponents, kp_df['TeamName'].tolist(), kp_df['Rank'].tolist())
    
    # 4. Apply Ranks and Calculate Stats
    box_df['ForensicScore'] = (box_df['eFG%']*2.0 - box_df['TO%']*1.5 + box_df['OR%']*0.5 + box_df['FTR']*0.3)
    baseline = box_df['ForensicScore'].mean()
    box_df['NetEff'] = box_df['ForensicScore'] - baseline
    
    # Classify Quadrants
    print("\n🔬 Classifying games...")
    
    def get_quadrant(opponent_rank, location):
        if location == 'Home':
            if opponent_rank <= 30: return 'Q1'
            elif opponent_rank <= 75: return 'Q2'
            elif opponent_rank <= 160: return 'Q3'
            else: return 'Q4'
        elif location == 'Neutral':
            if opponent_rank <= 50: return 'Q1'
            elif opponent_rank <= 100: return 'Q2'
            elif opponent_rank <= 200: return 'Q3'
            else: return 'Q4'
        else:  # Away
            if opponent_rank <= 75: return 'Q1'
            elif opponent_rank <= 135: return 'Q2'
            elif opponent_rank <= 240: return 'Q3'
            else: return 'Q4'

    box_df['Opp_Rank'] = box_df['Opponent'].map(rank_map)
    
    box_df['Quadrant'] = box_df.apply(
        lambda row: get_quadrant(row['Opp_Rank'], row['Location']) if pd.notna(row['Opp_Rank']) else None,
        axis=1
    )
    
    valid_games = box_df[box_df['Quadrant'].notna()].copy()
    print(f"   ✅ Successfully classified {len(valid_games)} games.")
    
    if len(valid_games) == 0:
        print("   ❌ STILL 0 GAMES. Check your CSV column names ('Opponent', 'Location').")
        return

    # 5. Build Profiles
    print("\n🏗️  Building Profiles...")
    
    def build_profile(df, location_filter, output_file):
        profiles = []
        teams = df['Team'].unique()
        
        for team in teams:
            team_games = df[df['Team'] == team]
            
            if location_filter == 'Home':
                loc_games = team_games[team_games['Location'] == 'Home']
            else:
                loc_games = team_games[team_games['Location'].isin(['Away', 'Neutral'])]
                
            if len(loc_games) < MIN_GAMES_MEDIUM_CONF: continue
            
            row = {'Team': team, 'Total_Games': len(loc_games)}
            row['Overall_NetEff'] = round(loc_games['NetEff'].mean(), 2)
            
            # Quadrant splits
            for quad in ['Q1', 'Q2', 'Q3', 'Q4']:
                q_games = loc_games[loc_games['Quadrant'] == quad]
                n = len(q_games)
                row[f'{quad}_Games'] = n
                
                if n >= MIN_GAMES_MEDIUM_CONF:
                    raw = q_games['NetEff'].mean()
                    w = n / (n + BAYESIAN_PRIOR_WEIGHT)
                    shrunk = (w * raw) + ((1-w) * row['Overall_NetEff'])
                    
                    row[f'{quad}_NetEff'] = round(raw, 2)
                    row[f'{quad}_NetEff_Shrunk'] = round(shrunk, 2)
                    row[f'{quad}_Confidence'] = 'HIGH' if n >= MIN_GAMES_HIGH_CONF else 'MEDIUM'
                else:
                    row[f'{quad}_NetEff'] = np.nan
                    row[f'{quad}_NetEff_Shrunk'] = np.nan
                    row[f'{quad}_Confidence'] = 'LOW'
            
            profiles.append(row)
            
        pd.DataFrame(profiles).to_csv(output_file, index=False)
        print(f"   💾 Saved {output_file} ({len(profiles)} teams)")

    build_profile(valid_games, 'Home', OUTPUT_HOME)
    build_profile(valid_games, 'Road', OUTPUT_ROAD)
    print("\n✅ DONE.")

if __name__ == "__main__":
    generate_validated_home_road_data()
