from .base_step import Step
import boto3
import urllib3
import time


@Step(
    type="pipeline_queue_executor",
    props_schema={
        "type" : "object",
        "properties" : {
            "sleep_time" : {"type" : "integer"},
            "total_wait_time": {"type" : "integer"},
            "dependent_stepfunction_name": {"type" : "string"}
        },
        "required":["sleep_time","total_wait_time"]
    }
)

class PipelineQueueExecutor :

    def get_execustion_list(self,arn_name_list,currnt_arn):
        executions_list=[]
        top_arn=""
        temp_time=""
        for arn_name in arn_name_list:
            response = self.client.list_executions(
            stateMachineArn=arn_name,
            statusFilter='RUNNING',
            maxResults=123)
            status_list = response["executions"]
        
            for i in status_list:
                executions_list.append(i["executionArn"])
                if temp_time == '':
                    top_arn = i["executionArn"]
                    temp_time = i["startDate"]
                if i["startDate"] < temp_time:
                    temp_time = i["startDate"]
                    top_arn = i["executionArn"]
        
        try:
            executions_list.remove(currnt_arn)
        except:
            pass
        
        return executions_list,top_arn

    def list_stat_machine_arn(self):
        response = self.client.list_state_machines(
        maxResults=600
        )
        statemachine_list = response["stateMachines"]

        if statemachine_list == []:
            raise Exception("Cannot Find Step Function List")

        return statemachine_list


    def get_arn(self,arn_list,step_function_name):
        final_arn_list=[]
        ind = 0
  
        if step_function_name is None:
            print("No input provided for step function name")
            step_name_list = [""]   
            ind = 1      
        else:
            step_name_list_temp = step_function_name.split(",")
            step_name_list = [ls+self.stage_variable for ls in step_name_list_temp]
    
        
        for step_function_details in arn_list:
            if step_function_details["name"] in step_name_list:
                ind = ind + 1
                final_arn_list.append(step_function_details["stateMachineArn"])
            else:
                pass
        print(ind,step_name_list,len(step_name_list))
        if ind == 0 or ind != len(step_name_list):
            raise Exception("Cannot find Step function ARN for one or more of the step function name -->"+str(step_name_list))
        else:
            return final_arn_list

    def wait_function(self,all_exec_list,all_dependednt_arn,top_exec_id,sleep_time,total_sleep_time,currnt_exec_id,iteration_num=0,):
        if all_exec_list == []:
            print("No dependent or other current step function running")
            return True
        else:
            print("checking top most execution ID for the current step function")
            if currnt_exec_id == top_exec_id:
                print("Current Instance of step function is First Step function so moving forward")
                print("Continuing Without waiting")
                return True

                
        iteration_num = iteration_num+1
        overall_wait = iteration_num*sleep_time
        if overall_wait > total_sleep_time:
            raise Exception("Overall waiting time excedeed the provided total_wait_time -->" +str(total_sleep_time) +", Exiting the step function")
        else:
            time.sleep(sleep_time)
            print("waiting for"+str(sleep_time)+" sec")
            all_run_exec,top_exec_id = self.get_execustion_list(all_dependednt_arn,currnt_exec_id)
            self.wait_function(all_run_exec,all_dependednt_arn,top_exec_id,sleep_time,total_sleep_time,currnt_exec_id,iteration_num)
        

    def run_step(self, spark, config, context=None, glueContext=None) :
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.client = boto3.client("stepfunctions")
        job_name = config.args.get('JOB_NAME')
        total_sleep_time = self.props.get("total_wait_time")
        sleep_time = self.props.get("sleep_time")
        dependent_step_function = self.props.get("dependent_stepfunction_name")
        current_step_function = config.args.get("step_function_name")
        self.stage_variable = config.args.get('STAGE')
        current_execution_id = config.args.get('JOB_FLOW_ID')
       
        print(sleep_time,"sleeptime")
        print(total_sleep_time,"total_sleep_time")
        print(dependent_step_function,"dependent_step_function")
        print(current_step_function,"current_step_function")
        print(job_name,"job_name")
        print(self.stage_variable,"stage_variable")
        print(current_execution_id,"current_execution_id")
       

        stete_machine_list=self.list_stat_machine_arn()
        current_stepfn_arn = self.get_arn(stete_machine_list,current_step_function.replace(self.stage_variable,""))
        dependent_stepfn_arn = self.get_arn(stete_machine_list,dependent_step_function)
        
        print(current_stepfn_arn,"current_arn")
        print(dependent_stepfn_arn,"dependednt_arn")

        all_arn = current_stepfn_arn + dependent_stepfn_arn
        print(all_arn,"--->all_arn")

        running_executions,top_execution_id =self.get_execustion_list(all_arn,current_execution_id)


        print(running_executions,"all_running_execuitions")
        print(top_execution_id,"top_execution_id")

        rslt = self.wait_function(running_executions,all_arn,top_execution_id,sleep_time,total_sleep_time,current_execution_id)
        print('Moving to Glue job')
