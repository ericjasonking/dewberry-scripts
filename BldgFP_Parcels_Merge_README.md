#BldgFP\_Parcel\_Merge.py
##OVERVIEW
>This script has two primary functions: merging parcels by state (inputs stored by county and spatially joining merged parcels to building footprints.

##WORKFLOW
###INITIAL PREPARATION STEPS
- Update global variables:

>>**1.** **BUILDING\_FP\_GDB** - path to the gdb containing the building footprints by state (should be named as {STATE}\_Poly)

>>**2.** **PARCEL\_DIR** - path to the directory containing the parcel gdb's by state

>>**3.** **PROJECT\_FOLDER** - path to the directory where outputs will be stored
- Confirm building footprint inputs are saved as {STATE}\_Poly

###SCRIPT DETAILS
- Input data:

>>**1.** FEMA Parcels (by county)

>>**2.** Bing Building Footprints (by state)

- Output data:

>>**1.** Merged state FEMA parcels (by state)

>>**2.** Spatially joined building footprints and parcels (by state)

- Functions:

>>**1.** **merged\_parcels(gdb)** - Merges county parcels by state and adds RES_NONRES field 

>>**2.** **bldg\_fp\_join(state)** - Spatially joins parcels to building footprints. First, joins based on center. Second, joins the missed footprints by intersection and removes the duplicates from the intersection (keeps the join with largest intersecting area)

>>**3.** **main()** - In this order: merges parcels, joins building footprints and parcels, copies outputs to final gdb and then deletes the intermediate features
