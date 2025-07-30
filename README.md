### Udacity STEDI Project

## Contents

+ [Implementation/Evidences](#Implementation/Evidences)


### Implementation/Evidences
<details>
<summary>
Landing Zone
</summary>

 The raw data is stored in the landing tables. These tables got create with SQL DDL transactions.
 
 [accelerometer_landing.sql](SQL_Tables/accelerometer_landing.sql)
 ![alt text](SQL_Tables/accelerometer_landing.png)
 
 
 [customer_landing.sql](SQL_Tables/customer_landing.sql)
 ![alt text](SQL_Tables/customer_landing.png)
 
 
 [step_trainer_landing.sql](SQL_Tables/step_trainer_landing.sql)
 ![alt text](SQL_Tables/step_trainer_landing.png)

These are AWS Glue Tables and can get queried by AWS Athena

Row Count:
 ![alt text](SQL_Tables/screenshot_tableRows.png)

All Customers are currently included even they not agreed to share their data for research purposes:
![alt text](SQL_Tables/screenshot_shareWithResearchAsOfDate.png)

</details>

<details>
<summary>
Trusted Zone
</summary>

# Customer

The Customer trusted table is created via an AWS Glue script [customer_landing_to_trusted.py](customer/trusted/customer_landing_to_trusted.py). Only Customers who agreed to share their data for research purposes are included. 
![alt text](customer/trusted/Screenshot_sharewithresearchasofdate.png) 
![alt text](customer/trusted/customer_trusted.png)


# Accelerometer

The Accelerometer trusted table is created via the following AWS Glue script [accelerometer_landing_to_trusted.py](accelerometer/trusted/accelerometer_landing_to_trusted.py).
Its only the data included after they agreed to share their data. 
![alt text](accelerometer/trusted/accelerometer_trusted.png)

# Step Trainer

The Step Trainer trusted table is created via the following AWS Glue script [step_trainer_landing_to_trusted.py](step_trainer/trusted/step_trainer_landing_to_trusted.py). 
There is only the data keept that has accelerometer data and the customer agreed to share it. 
![alt text](step_trainer/trusted/step_trainer_trusted.png)
Note: The step trainer trusted table has the row count because I used the customer curated table. If I would use the customer trusted table the row count would be 14460.

## Row Count Evidence
![alt text](trusted_row_count.png)
Note: The step trainer trusted table has the row count because I used the customer curated table. If I would use the customer trusted table the row count would be 14460. 
</details>

<details>
<summary>
Curated Zone
</summary>

# Customer

The Customer curated table has only the customer included that have accelerometer data and the customer agreed to share the data.
Its also GDPR complaint because personal data is removed. Its created via the following script
[customer_trusted_to_curated.py](customer/curated/customer_trusted_to_curated.py).
![alt text](customer/curated/customers_curated.png)

# Maschine Learning

This table has the step trainer and the accelerometer data from customer who agreed to share their data. 
Personal informations are removed. Its created via the following script
[Machine_learning_curated.py](step_trainer/curated/Machine_learning_curated.py).
The glue table is called machine_learning_curated.
![alt text](step_trainer/curated/machine_learning_curated.png)

## Row Count Evidence
![alt text](curated_row_count.png)

 
</details>
