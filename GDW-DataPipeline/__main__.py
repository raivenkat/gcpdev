from src.Gdwdataloader import GdwDataLoaderApplication
import sys



if __name__ == '__main__':
    print("-----------  Process Started ----------------")
    raw_zone_path = sys.argv[1]
    target_table = sys.argv[2]
    print(f'args--------<>{raw_zone_path}<>{target_table}' )
    print('Processing for table =====>',target_table)
    processor_obj = GdwDataLoaderApplication(raw_zone_path,target_table)
    processor_obj.run()
    print('Process Completed for the table ============> ',target_table)