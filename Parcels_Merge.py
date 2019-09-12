import arcpy
import os
import multiprocessing as mp
from datetime import datetime

### UPDATE PATHS ###
BUILDING_FP_GDB = r'T:\CCSI\TECH\FEMA\Datasets\Bldg_Footprints\Bing\July_2018\bldgftprnts_20180716.gdb'
PARCEL_DIR = r'T:\CCSI\TECH\FEMA\Datasets\FEMA_Parcels\Processed\Parcels_July2019'
PROJECT_FOLDER = r'P:\Temp\eking\Building_FP_Parcels'


state_list = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "District_Of_Columbia", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", 
                   "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
                   "New_Hampshire", "New_Jersey", "New_Mexico", "New_York", "North_Carolina", "North_Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode_Island", "South_Carolina", "South_Dakota",
                   "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West_Virginia", "Wisconsin", "Wyoming"]

state_abbr = {"Alabama":"AL", "Alaska":"AK", "Arizona":"AZ", "Arkansas":"AR", "California":"CA", "Colorado":"CO", "Connecticut":"CT", "Delaware":"DE", "District_Of_Columbia":"DC", "Florida":"FL", "Georgia":"GA", "Hawaii":"HI", "Idaho":"ID", "Illinois":"IL", 
                   "Indiana":"IN", "Iowa":"IA", "Kansas":"KS", "Kentucky":"KY", "Louisiana":"LA", "Maine":"ME", "Maryland":"MD", "Massachusetts":"MA", "Michigan":"MI", "Minnesota":"MN", "Mississippi":"MS", "Missouri":"MO", "Montana":"MT", "Nebraska":"NE", "Nevada":"NV",
                   "New_Hampshire":"NH", "New_Jersey":"NJ", "New_Mexico":"NM", "New_York":"NY", "North_Carolina":"NC", "North_Dakota":"ND", "Ohio":"OH", "Oklahoma":"OK", "Oregon":"OR", "Pennsylvania":"PA", "Rhode_Island":"RI", "South_Carolina":"SC", "South_Dakota":"SD",
                   "Tennessee":"TN", "Texas":"TX", "Utah":"UT", "Vermont":"VT", "Virginia":"VA", "Washington":"WA", "West_Virginia":"WV", "Wisconsin":"WI", "Wyoming":"WY"}


def merge_parcels(gdb):
    '''
    Merge parcels by state and adds field to tell if parcel is Residential or Non-Residential
    '''  
    feature_list = []
    state = gdb.split('\\')[-1].split('.')[0]
    try:
        arcpy.env.workspace = gdb
        arcpy.env.overwriteOutput = True
        datasets = arcpy.ListDatasets()
        for dataset in datasets:
            dataset_path = os.path.join(gdb, dataset) # COUNTIES
            arcpy.env.workspace = dataset_path
            features = arcpy.ListFeatureClasses(feature_type = 'Polygon')
            for feature in features:
                # add res/nonres field
                if '_Non_Res' in feature:
                    arcpy.AddField_management(feature, 'RES_NONRES', field_type='TEXT', field_length=10)
                    arcpy.CalculateField_management(feature, 'RES_NONRES', """'{}'""".format('Non_Res'), 'PYTHON')
                    feature_list.append(os.path.join(dataset_path, feature))
                elif '_Res' in feature:
                    arcpy.AddField_management(feature, 'RES_NONRES', field_type='TEXT', field_length=10)
                    arcpy.CalculateField_management(feature, 'RES_NONRES', """'{}'""".format('Res'), 'PYTHON')
                    feature_list.append(os.path.join(dataset_path, feature))
                else:
                    print 'Unknown feature'
        
        # merge all parcels by state
        arcpy.env.workspace = gdb
        arcpy.env.overwriteOutput = True
        arcpy.Merge_management(feature_list, os.path.join(gdb,'{}_Parcels_Merged'.format(state)))
        return '{}: SUCCESS!'.format(state)
    except Exception as e:
        return '{}: ERROR...\n{}'.format(state, e) 

        
def bldg_fp_join(state):
    '''
    Spatially joins parcels to building footprints. First, joins based on center. Second, joins the missed footprints
    by intersection and removes the duplicates from the intersection (keeps the one with largest intersecting area)
    '''
    bldg_fp_path = os.path.join(BUILDING_FP_GDB, '{}_poly'.format(state.replace('_', '')))
    parcel_path = os.path.join(PARCEL_DIR, '{}.gdb'.format(state), '{}_Parcels_Merged'.format(state))
    workspace_path = os.path.join(PROJECT_FOLDER, '{}.gdb'.format(state))
    if not os.path.exists(workspace_path):
        arcpy.CreateFileGDB_management(PROJECT_FOLDER, '{}.gdb'.format(state))
        
    arcpy.env.workspace = workspace_path
    arcpy.env.overwriteOutput = True
    arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(102008) # North_America_Albers_Equal_Area_Conic
    
    try:
        ### copy features to workspace as shapefiles
        arcpy.FeatureClassToFeatureClass_conversion(bldg_fp_path, workspace_path, 'Bldg_FP')
        arcpy.FeatureClassToFeatureClass_conversion(parcel_path, workspace_path, 'Merged_Parcels')
        
        ### Spatial join - HAVE_THEIR_CENTER_IN
        arcpy.SpatialJoin_analysis('Bldg_FP.', 'Merged_Parcels', 'Bldg_FP_Parcels', match_option = 'HAVE_THEIR_CENTER_IN')
        
        ### Select and remove unjoined building footprints
        arcpy.Select_analysis('Bldg_FP_Parcels', 'selection', where_clause = '"Join_Count" = 0')
        arcpy.FeatureClassToFeatureClass_conversion('selection', workspace_path, 'Missing_FP')
        
        # delete fields and failed joins
        exclude = ['OBJECTID', 'Shape', 'uniqueid', 'Shape_Length', 'Shape_Area']
        for f in arcpy.ListFields('Missing_FP'):
            field = f.name
            if field not in exclude: 
                arcpy.DeleteField_management('Missing_FP', field)
        
        arcpy.MakeFeatureLayer_management('Bldg_FP_Parcels', 'tempLayer')
        arcpy.SelectLayerByAttribute_management('tempLayer', "NEW_SELECTION", '"Join_Count" = 0')
        if int(arcpy.GetCount_management('tempLayer').getOutput(0)) > 0:
            arcpy.DeleteFeatures_management('tempLayer')
        arcpy.DeleteField_management('Bldg_FP_Parcels', 'Join_Count')
        arcpy.DeleteField_management('Bldg_FP_Parcels', 'TARGET_FID')
        arcpy.DeleteField_management('Bldg_FP_Parcels', 'FID_1')
        
        # intersect missing footprints with parcels
        arcpy.Intersect_analysis(['Missing_FP', 'Merged_Parcels'], 'intersection')
        
        # delete unneeded fields and duplicates (keep one with largest area)
        sql_orderby = 'ORDER BY uniqueid DESC, Shape_Area DESC'
        temp_id = ''
        temp_area = 0
        count = 0
        with arcpy.da.UpdateCursor('intersection', ['uniqueid', 'Shape_Area'], sql_clause = (None, sql_orderby)) as cursor:
            for row in cursor:
                if temp_id == '':
                    temp_id = row[0]
                    temp_area = row[1]
                else:
                    if temp_id == row[0] and temp_area >= row[1]:
                        cursor.deleteRow()
                        count = count + 1
                    else:
                        temp_id = row[0]
                        temp_area = row[1]
        
        arcpy.DeleteField_management('intersection', 'FID_Missing_FP')   
        arcpy.DeleteField_management('intersection', 'FID_Merged_Parcels')
                
        # join to missing footprints
        arcpy.JoinField_management("Missing_FP", "uniqueid", "intersection", "uniqueid")
        arcpy.DeleteField_management('Missing_FP', 'uniqueid_1')
        
        # merge to first join
        arcpy.Append_management('Missing_FP', 'Bldg_FP_Parcels')

        return '{}: SUCCESS!'.format(state)
    except Exception as e:
        return '{}: ERROR...\n{}'.format(state, e) 

def main():
    todays_date = str(datetime.now()).split(' ')[0].replace('-', '')
    
    # MERGE PARCELS
    print('MERGING PARCELS...')
    gdb_list = []
    arcpy.env.workspace = PARCEL_DIR
    workspaces = arcpy.ListWorkspaces()
    for workspace in workspaces:
        print workspace
        if os.path.basename(workspace).split('.')[0] in state_list:
            gdb = os.path.join(PARCEL_DIR, workspace) # STATES
            gdb_list.append(gdb)
    print gdb_list
    num_processes = 4
    p = mp.Pool(num_processes)
    output = p.map(merge_parcels, gdb_list)
    p.close()
    for line in output:
        print line
        
    # JOIN BLDG FP AND PARCELS
    print('JOINING PARCELS TO BUILDING FOOTPRINTS...')
    num_processes = 4
    p = mp.Pool(num_processes)
    output = p.map(bldg_fp_join, state_list)
    p.close()
    p.join()
    for line in output:
        print line
    
    # COPY FEATURES TO GDB and compare input and output building footprints
    print('COPYING TO FINAL GDB...')
    final_gdb_name = 'BldgFP_Parcels_{}.gdb'.format(todays_date)
    final_gdb_path = os.path.join(PROJECT_FOLDER, final_gdb_name)
    arcpy.env.workspace = final_gdb_path
    arcpy.env.overwriteOutput = True
    if not os.path.exists(final_gdb_path):
        arcpy.CreateFileGDB_management(PROJECT_FOLDER, final_gdb_name)
    for state in state_list:
        in_bfp = os.path.join(BUILDING_FP_GDB, '{}_poly'.format(state.replace('_', '')))
        state_gdb = os.path.join(PROJECT_FOLDER, '{}.gdb'.format(state))
        out_bfp = os.path.join(PROJECT_FOLDER, '{}.gdb\Bldg_FP_Parcels'.format(state))
        in_count = int(arcpy.GetCount_management(in_bfp).getOutput(0))
        out_count = int(arcpy.GetCount_management(out_bfp).getOutput(0))
        if in_count == out_count:
            print('{}: EQUAL!'.format(state))
            out_feature = '{}_Bldg_FP_Parcels'.format(state_abbr[state])
            arcpy.FeatureClassToFeatureClass_conversion(out_bfp, final_gdb_path, out_feature)
            if in_count != int(arcpy.GetCount_management(out_feature).getOutput(0)):
                print('ERROR COPYING FEATURE: {}'.format(out_feature))
            else:
                # deletes intermediate feature classes/gdb after final BLDG FP PARCEL join is saved in final gdb
                print('Deleting gdb')
                arcpy.Delete_management(state_gdb)
        if in_count != out_count:
            print('{}: UNEQUAL...'.format(state))
            print('In count = {}'.format(in_count))
            print('Out count = {}'.format(out_count))

if __name__ == '__main__':
    main()

    
