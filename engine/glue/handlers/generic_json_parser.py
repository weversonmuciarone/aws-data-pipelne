from .base_step import Step
import json
from pyspark.sql.types import StringType
import sys
from datetime import datetime
from engine.glue.handlers.base_central_logging import BaseCentralLogging


@Step(
    type="generic_json_parser",
    props_schema={
        "type": "object",
        "properties": {
            "output_bucket": {"type": "string"},
            "prefix" : {"type": "string"},
            "error_bucket": {"type": "string"},
            "error_prefix" : {"type": "string"},
            "flat_file_write" : {"type": "string"},
            "field_function" : {"type": "string"},
            "target" : {"type": "string"},
            "options":{
                "type":"object",
                "properties":{
                    "field_update": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    },                
                    "char_remove": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    },
                    "central_bucket" : {"type" : "string"}
                },
                "required":["central_bucket"],
            }
        },
        "required":["flat_file_write","error_bucket","error_prefix","target","field_function"]
    }
)



class GenericJsonParser:

    def get_random(self,val):
        micro_second = datetime.now().microsecond
        return str(val)+"~M"+str(micro_second)

    def get_field_name(self,path,name_list,remove_list=[]):

        if remove_list != []:
            for i in remove_list:
                path = path.replace(i,"")
    
        path = path.lower()

        if "/" in path:
            head,tail = path.rsplit("/",1)
        else:
            tail = path
        tail = tail.strip().replace("@","atr_").replace(":","_")
        
        if tail not in name_list:
            
            name_list.append(tail)
            return(tail,name_list)
        elif tail in name_list:
            if "/" in path:
                new_path = head+"_"+tail
            else:
                new_path = path+"_1"
                
            name2,name_list=self.get_field_name(new_path,name_list,remove_list)
            return(name2,name_list)

    def create_data(self,path,field_lst,tbl_list,rec,all_data,text=''):
        rec.append(json.dumps(all_data))
        new_path = path.rsplit("/",1)[0]
        new_dic= dict(all_data)
        
        for i in new_dic:   
            if path in i:         
                all_data.pop(i,None)
        return(rec,all_data,field_lst,tbl_list)


    def parse(self,path,json_obj,field_list,tbl_list,all_data,rec_list):

        if isinstance(json_obj,list):
            json_list = json_obj
            tbl_list.append(path)
            for fields in json_list: 
                if isinstance(fields,list):
                    tbl_list.append(path)
                    field_list.append(path)
                    field_list,tbl_list,all_data,rec_list = self.parse(path,fields,field_list,tbl_list,all_data,rec_list)

                if isinstance(fields,dict):
                    field_list,tbl_list,all_data,rec_list = self.parse(path,fields,field_list,tbl_list,all_data,rec_list)

                else:
                    if fields is not None:          
                        all_data[path] = fields

                if path in tbl_list:
                        rec_list,all_data,field_list,tbl_list=self.create_data(path,field_list,tbl_list,rec_list,all_data)
                        
        if isinstance(json_obj,dict):
            json_dic=json_obj

            for key,value in json_dic.items():
                f_path = str(path+"/"+key)
                
                field_list.append(f_path)
                if isinstance(value,list):
                    field_list,tbl_list,all_data,rec_list = self.parse(path+"/"+key,value,field_list,tbl_list,all_data,rec_list)
                
                elif isinstance(value,dict):
                    field_list,tbl_list,all_data,rec_list = self.parse(path+"/"+key,value,field_list,tbl_list,all_data,rec_list)                              
                else:
                    f_path = path+"/"+key
                    
                    field_list.append(f_path)
                    
                    if value is not None:
                        # if f_path in self.unique_record:
                        if f_path.endswith('peci:Worker/peci:Effective_Change/peci:Position/peci:Position_ID'):
                            value = self.get_random(value)
                            all_data[f_path]=value
                        else:
                            all_data[f_path]=str(value)

        tbl_list.append("/root")
        if path == "/root":  
            rec_list,all_data,field_list,tbl_list=self.create_data(path,field_list,tbl_list,rec_list,all_data)

        return(field_list,tbl_list,all_data,rec_list)

    def process_json(self,rec):
        single_rec = rec[0]
        try:
            data = json.loads(single_rec)
            fld,tbl,all_d,rec_list=self.parse("/root",data,[],[],{},[])
            fld= list(set(fld))
            return("correct",fld,rec_list)

        except Exception as e:
            print("invalid record ",single_rec)
            return("malformed",single_rec)
        return rec_list

    def finalize_json(self,field_dic):
        def _finalize_json(rec2):
            new={}
            for i in rec2:
                fld_name = field_dic[i]
                new[fld_name] = rec2[i]
            return new
        
        return _finalize_json


    def generic_create_fields_history(self,fld_pth_list,replace_list,fld_upd_list):
        fld_pth_list = list(set(fld_pth_list))
        fld_pth_list.sort()

        field_name_dict={}
        name_list=[]
        for pth in fld_pth_list:
            if "/" in pth:
                check_pth = pth.rsplit("/",1)[1]
                if check_pth in fld_upd_list:
                    n_path = pth.rsplit("/",1)[0]+"_"+check_pth
                else:
                    n_path = pth

                n_fld,name_list=self.get_field_name(n_path,name_list,replace_list)
                if pth in field_name_dict:
                    print("Please check this for issue")
                else:
                    field_name_dict[pth] = n_fld
            else:
                field_name_dict[pth] = pth
        return field_name_dict

    def custom_create_fields_history(self,fld_pth_list,replace_list,fld_upd_list):
        fld_pth_list = list(set(fld_pth_list))
        fld_pth_list.sort()
        field_name_dict={}
        name_list=[]
        for pth in fld_pth_list:
            if "/" in pth:
                if pth.endswith("/$"):
                    npth = pth.rsplit("/",1)[0]
                    if npth in field_name_dict:
                        n_fld = field_name_dict[npth]
                        field_name_dict[pth] = n_fld
                    else:
                        check_pth = npth.rsplit("/",1)[1]
                        n_path = npth.rsplit("/",1)[0]+"_"+check_pth
                        n_fld,name_list=self.get_field_name(n_path,name_list,replace_list)
                        if pth in field_name_dict:
                            print("Please check this for issue")
                            sys.exit()
                        else:
                            field_name_dict[pth] = n_fld
                else:
                    if pth+"/$" in field_name_dict:
                        n_fld = field_name_dict[pth+"/$"]
                        field_name_dict[pth] = n_fld
                    else:
                        new_pth = pth
                        for upd_fld in fld_upd_list:
                            if new_pth.endswith(upd_fld):
                                head,tail = new_pth.rsplit("/",1)
                                new_pth = head+"_"+tail

                        check_pth = new_pth.rsplit("/",1)[1]
                        n_path = new_pth.rsplit("/",1)[0]+"_"+check_pth
                        n_fld,name_list=self.get_field_name(n_path,name_list,replace_list)
                        if pth in field_name_dict:
                            print("Please check this for issue")
                            sys.exit()
                        else:
                            field_name_dict[pth] = n_fld
            else:
                field_name_dict[pth] = pth

        return field_name_dict

    def save_df(self,dataframe,bckt,prfx,ftype):
        today = datetime.today()

        filename= ftype+"_"+str(today).replace(" ","-")

        path = f"s3://{bckt}/{prfx}/{filename}"
        
        print(path)
        try:
            dataframe.repartition(1).write.format('com.databricks.spark.csv') \
                .mode('overwrite').option("header", "true").option("ignoreLeadingWhiteSpace", False).option(
                "ignoreTrailingWhiteSpace", False) \
                .save(path)
            return True
        except:
            print("failed to write file")
            return False



    def run_step(self, spark, config, context=None, glueContext=None):
        job_name = config.args.get('JOB_NAME')
        output_bucket = self.props.get("output_bucket","")
        prefix = self.props.get("prefix","")
        error_bucket = self.props.get("error_bucket")
        error_prefix = self.props.get("error_prefix")
        field_function = self.props.get("field_function")       
        remove_list =  self.props.get("options",{}).get("char_remove",[])
        field_update_list =  self.props.get("options",{}).get("field_update",[])
        flat_file_write =  self.props.get("flat_file_write","no")
        self.unique_record = ["/root/peci:Workers_Effective_Stack/peci:Worker/peci:Effective_Change/peci:Position/peci:Position_ID"]
        print(remove_list)
        print(field_update_list)

        target_df = context.df(self.props.get("target"))
        target_rdd = target_df.rdd

        all_records = target_rdd.map(self.process_json).filter(bool)
        all_records = all_records.cache()

        malformed_records = all_records.filter(lambda x : x[0] == "malformed").map(lambda x:x[1])
        malformed_df = spark.createDataFrame(malformed_records,  StringType())
        
        if len(malformed_df.head(1)) != 0:
            print("malformed records found")
            out = self.save_df(malformed_df,error_bucket,error_prefix,"malformed")
            timestamp = str(datetime.now())
            guid = (config.args.get('JOB_FLOW_ID')).split(":")[-1]
            log_info = {
                "timestamp": timestamp,
                "guid": guid,
                "process": config.args.get("step_function_name"),
                "job": config.args.get("JOB_NAME"),
                "step": self.name,
                "eventType": "Pipeline-Job-Malformed-Records",
                "status": "WARNING",
                "msg": "Malformed records found in json input file"
            }
            base_central_logger = BaseCentralLogging()
            central_logging_bucket=self.props.get("options",{}).get("central_bucket","")    
            base_central_logger.create_log_in_s3(config, log_info,central_logging_bucket)
            # malformed_df.show()

        success_records = all_records.filter(lambda x : x[0] == "correct")
        success_records= success_records.cache()

        field_records = success_records.flatMap(lambda x:x[1]).collect()

        if field_function.lower() == "generic":
            field_name_dict = self.generic_create_fields_history(field_records,remove_list,field_update_list)
        else:
            field_name_dict = self.custom_create_fields_history(field_records,remove_list,field_update_list)
        print(json.dumps(field_name_dict))


        records = success_records.flatMap(lambda x:x[2]).map(lambda y: {} if y == '{}' else json.loads(y)) \
                        .filter(lambda rec : rec !={}).map(self.finalize_json(field_name_dict))  

        dataframe = spark.createDataFrame(records.collect())
        dataframe = dataframe.dropDuplicates()
        print("_____________________________________________")
        
        if output_bucket != '' and prefix != '' and flat_file_write.lower() == 'yes':
            out = self.save_df(dataframe,output_bucket,prefix,"flat_file")
        else:
            print("Check Input Parameters for writing file")
            print(output_bucket,prefix,flat_file_write)
            
        dataframe.createOrReplaceTempView(self.name)
        context.register_df(self.name, dataframe)
