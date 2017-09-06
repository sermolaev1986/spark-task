for local run you can use the following parameters:

JVM options:
 
`-Dspark.master=local[*]`

command line args:

"input" "," "[{\"existing_col_name\" : \"name\", \"new_col_name\" : \"first_name\", \"new_data_type\" : \"string\"},{\"existing_col_name\" : \"age\", \"new_col_name\" : \"total_years\", \"new_data_type\" : \"integer\"}]"

Description of args: 

dataPath - path to data folder
 
delimiter - delimiter used to separate columns in csv file

columnConfigs - json column configuration