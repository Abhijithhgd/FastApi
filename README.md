######  ################################################################################

###

### V1.0 07/24/2022  This program will call relevant procedures to prepare data for activation files

###

######  #####################################################################################

import json 
import boto3 
import sys 
import time 
from awsglue.utils import getResolvedOptions 
import logging 
import pgdb

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

args = getResolvedOptions(sys.argv, ['Secret','Env'])

secret = args['Secret']
env= args['Env']

print(env)

# print(sgc_redshift_database)

print('Environ is: %s' % env)
print('Secret is: %s' % secret)

def ses_te_error(status, message):
   # log_details("Send the messages"+ str(key),"Information") 
    ses = boto3.client('ses')
    s3 = boto3.client('s3')
    print('in send notification')
    
    try:
        response = s3.get_object(
            Bucket= 'osds-2-0-internal-us-west-2-2019073017244408960000000e',
            #'osds2-stage-internal-us-west-2',
            Key= 'storage/pii_dedup_triggers/email.config'
            )
        contents = response['Body'].read()
        print (contents)
        #print(type(contents))
        dic_y= json.loads(contents)
        #print(dic_y)
        #print(type(dic_y))
        from_email = dic_y['from_email']
        toemail = dic_y['toemail']
        print(contents)
        print(contents[0])
       
    except Exception as e:
        print(e)
        from_email= 'SISC.GISC-INOSDS.AlertMon@ap.sony.com'
        toemail= ['Abhijith.Hegde@sony.com']
    
    #key =filename
    
    BODY_HTML = """\
            <html>
            <style>
            table, tr, td, th{
            border: 1px solid black;
            width: '100%';
            white-space: nowrap;
            }
            th{
            background-color:#27AE60 ;
            }
            </style>
            <body>"""


response = ses.send_email(
        #Source='manjunath.nagaraj@sony.com',
        Source=from_email,
        Destination={
            'ToAddresses':toemail
            },
        Message={
            'Subject': {
                'Data': status,
                'Charset': 'UTF-8',
                },
            'Body': {
                'Html': {
                    'Charset': 'UTF-8',
                    'Data': str(message),
                    },
                }
            }
        )

try:
print ('Getting Connection Info')

    secmgr = boto3.client('secretsmanager')
    secret = secmgr.get_secret_value(SecretId=secret)
    secretString = json.loads(secret["SecretString"])
    user = secretString["username"]
    password = secretString["password"]
    host = secretString["host"]
    port = secretString["port"]
    database = secretString["dbname"]
        
    print('Connecting to Redshift: %s' % host)
        
    try:        
        conn = pgdb.connect(database=database, host=host, user=user, password=password, port=port)  
        print(conn)
        print('Successfully Connected to Cluster')
        cursor = conn.cursor()
        cursor1 = conn.cursor() 
        query_get_act_id = "select distinct sgc_code from activation_general.aud_act_header a where exists (select 1  from activation_general.aud_act_platform_selection b where b.activation_status = 1002 and a.batch_id = b.batch_id and a.campaign_name = b.campaign_name)"
        cursor.execute(query_get_act_id)
        rows = cursor.fetchall()
        print("rows ::: {}".format(rows))
        print("rows ::: {}".format(rows[0][0]))
        sgc_code = rows[0][0]
        if sgc_code == "SPHE":
			print(sgc_code)
			try:
                print('-----------------Get details successfully-----------------')             

                cursor.execute('select * from ');            

                print('-------------stage9 procedure stg9_curr_act_data_sphe start-----------------------')
                cursor.execute('truncate table activation_general.aud_act_curr_act_data');

                cursor.execute('CALL activation_general.stg9_curr_act_data_sphe()')
                print('-------------stage9 procedure stg9_curr_act_data_sphe complete-----------------------')

                conn.commit() 

                print('-------------stage9 procedure stg9_curr_act_detail start-----------------------')
                cursor.execute('CALL activation_general.stg9_curr_act_detail()')
                print('-------------stage9 procedure stg9_curr_act_detail complete-----------------------')

                conn.commit() 

                print('-------------stage9 procedure stg9_pii_dedup start-----------------------')
                cursor.execute('truncate table activation_general.tmp_non_pii_dedup');
                            
                cursor.execute('CALL activation_general.stg9_pii_dedup()')
                print('-------------stage9 procedure stg9_pii_dedup complete-----------------------')

                conn.commit() 

                print('-------------stage9 procedure stg9_threshold_cal start-----------------------')
                cursor1.execute('CALL activation_general.stg9_threshold_cal()')
                conn.commit()
                result=[row[0] for row in cursor1]
                abort_ind = result[0]
                print ('Threshold Indicator: %s' %abort_ind)
                if  abort_ind == 'Y':
                    ses_te_error(str('[')+ str(env) +'] - Stage_9 Glue Failed due to Threshold Issue', 'Hi Team,\n\nThe stage 9 procedure in Glue job Failed due to Threshold Issue.\n\nRegards\nOSDS Team')
                    cursor1.close()
                    cursor.close() 
                    conn.close()
                    sys.exit(-1)
                print('-------------stage9 procedure stg9_threshold_cal complete-----------------------')

                print('-------------stage9 procedure stg9_get_pii_info start-----------------------')            
                cursor.execute('CALL activation_general.stg9_get_pii_info()')
                print('-------------stage9 procedure stg9_get_pii_info complete-----------------------')

                conn.commit()
                time.sleep(120)
                cursor1.close()            
                cursor.close() 
                conn.close()

                print('-----------------STAGE9 executed successfully-----------------') 

                ses_te_error(str('[')+ str(env) +'] - Stage_9 Glue Completed successfully', 'Hi Team,\n\n The stage 9 procedure in Glue job completed successfully. \n\n Regards \n OSDS Team')
				
			except Exception as e:
				print("Exception")
				ses_te_error(str('[')+ str(env) +'] -Error in  Stage_9', e)
				print('Error: {0}'. format(e))
				sys.exit(-1)
        elif (sgc_code == "SNA") :
		    print(sgc_code)
			try:
				print('-----------------STAGE9 START successfully-----------------')             
				
				cursor.execute('truncate table sphe_pii_def.stg9_campaign_pii_info');            
				
				print('-------------stage9 procedure stg9_curr_act_data_sna start-----------------------')
				cursor.execute('truncate table activation_general.aud_act_curr_act_data');
				
				cursor.execute('CALL activation_general.stg9_curr_act_data_sna()')
				print('-------------stage9 procedure stg9_curr_act_data_sna complete-----------------------')
				
				conn.commit() 
				
				print('-------------stage9 procedure stg9_curr_act_detail_sna start-----------------------')
				cursor.execute('CALL activation_general.stg9_curr_act_detail_sna()')
				print('-------------stage9 procedure stg9_curr_act_detail_sna complete-----------------------')
				
				conn.commit() 
				
		
				
				print('-------------stage9 procedure stg9_get_pii_info_sna start-----------------------')            
				cursor.execute('CALL activation_general.stg9_get_pii_info_sna()')
				print('-------------stage9 procedure stg9_get_pii_info_sna complete-----------------------')

				conn.commit()
				time.sleep(120)
				cursor.close() 
				conn.close()

				print('-----------------STAGE9 executed successfully-----------------') 

				ses_te_error(str('[')+ str(env) +'] - Stage_9 Glue Completed successfully', 'Hi Team,\n\n The stage 9 procedure in Glue job completed successfully. \n\n Regards \n OSDS Team')
				
			except Exception as e:
				print("Exception")
				ses_te_error(str('[')+ str(env) +'] -Error in  Stage_9', e)
				print('Error: {0}'. format(e))
				sys.exit(-1)
    except Exception as e:
        print(e)
        print('Connection Error: {0}'. format(e))
        sys.exit(-1)

except Exception as e:
print (e)
print("Error : {0}". format(e))
sys.exit(-1)
    