import os
import configparser
import boto3
from botocore.exceptions import NoCredentialsError
import snowflake.connector

class hms:
        
    def __init__(self):
        pass
    
    def upload_to_aws(self,s3_client,local_file, bucket, s3_file):
        # Retrieve AWS credentials from environment variables
        try:
            s3_client.upload_file(local_file, bucket, s3_file)
            print(f"Upload Successful: {s3_file}")
            return True
        except FileNotFoundError:
            print(f"The file was not found : {s3_file}")
            return False
        except NoCredentialsError:
            print("Credentials not available")
            return False
    
    def copy_all_local_s3(self,s3_bucket,directory,config_path,cred_path):
        local_path = directory
        config_file_path = config_path
        config = configparser.ConfigParser()
        config.read(config_file_path)
        
        config_cred = configparser.ConfigParser()
        config_cred.read(cred_path)
        
        if not os.path.isdir(directory):
            raise ValueError(f"Directory '{directory}' does not exists")
        #os.walk(top, topdown=True, onerror=None, followlinks=False)
        
        aws_access_key_id = config_cred['Care_Data_set_aws']['AWS_ACCESS_KEY_ID']
        aws_secret_access_key = config_cred['Care_Data_set_aws']['AWS_SECRET_ACCESS_KEY']
        aws_region = config_cred['Care_Data_set_aws']['AWS_DEFAULT_REGION']
        
        #aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        #aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        #aws_region = os.getenv('AWS_DEFAULT_REGION')
        
        #print('aws_access_key_id',aws_access_key_id)
        #print('AWS_SECRET_ACCESS_KEY',aws_secret_access_key)
        #print('AWS_DEFAULT_REGION',aws_region)
        s3_client = boto3.client('s3',
                                 aws_access_key_id=aws_access_key_id,
                                 aws_secret_access_key=aws_secret_access_key,
                                 region_name=aws_region)
        root_dir =  config['Care_s3_folder']['local_path']
        print('root_dir',root_dir)
        for section in config.sections():
            for key in config[section]:
                if section == 'Care_Data_set':
                    print(f"{key}={config[section][key]}")
                    local_file = root_dir+config[section][key]
                    s3_file = config['Care_s3_folder']['s3_path'] + config[section][key]
                    uploaded = self.upload_to_aws(s3_client,local_file, s3_bucket, s3_file)
        print()
        
        #for root_dir, dirs, files in os.walk(local_path):
        #    for file in files:
        #        if '.csv' in file:
        #            local_file = os.path.join(root_dir, file)
        #            local_file=local_file.replace('\\','/')
        #            uploaded = upload_to_aws(local_file, s3_bucket, file)

    def load_data_files_tables(self,config_path,cred_path):
        
        config_cred = configparser.ConfigParser()
        config_cred.read(cred_path)
        account = config_cred['Care_Data_set_sf']['sf_acct']
        usr = config_cred['Care_Data_set_sf']['sf_user']
        pwd = config_cred['Care_Data_set_sf']['sf_pwd']
        rol = config_cred['Care_Data_set_sf']['role']
        wh = config_cred['Care_Data_set_sf']['warehouse']
        db = config_cred['Care_Data_set_sf']['database']
        sch = config_cred['Care_Data_set_sf']['schema']
        
        try:
            print('connecting...')
            con = snowflake.connector.connect(
                user = usr,
                password = pwd,
                account = account,
                role = rol,
                warehouse = wh,
                database = db,
                schema = sch )
            print('connected....')
                        
            config = configparser.ConfigParser()
            config.read(config_path)
            script_path = config_cred['Care_Data_set_sf']['local_table_ddl_script_path']
            print('script_path---',script_path)
            for section in config.sections():
                for key in config[section]:
                    if section == 'Care_Data_set':
                        local_file = script_path +key+'.sql'
                        print('local_file',local_file)
                        with open(local_file,'r') as fp:
                            sql_script = fp.read()
                        print('sql script----',sql_script)
                        con.cursor().execute(sql_script)    
                        print('---table created----')
                        con.cursor().execute("""COPY INTO """+ key +""" FROM @"""+ config_cred['Care_Data_set_sf']['stage']+ 
                                             """ file_format = """+config_cred['Care_Data_set_sf']['file_format'] +
                                             """ files = ( '""" + config[section][key] + """')"""
                                             """ purge = """ +config_cred['Care_Data_set_sf']['purge'] +""";""" )
                        
                        print(f"{key}={config[section][key]}")
        except Exception as e:
            print(e)
        finally:
            con.cursor().close();
h = hms()
#h.copy_all_local_s3(s3bucket,path of csv files,configfile, credentialsconfigfile)
s = h.copy_all_local_s3('sfawsstg','D:/HMS','D:/config.ini','D:/cred_config.ini')
#h.load_data_files_tables(configfile, credentialsconfigfile)
d= h.load_data_files_tables('D:/config.ini','D:/cred_config.ini')

