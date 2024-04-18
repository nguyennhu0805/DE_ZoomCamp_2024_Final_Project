# DE_ZoomCamp_2024_Final_Project
**1. Problem Define**

In this section, I will outline the problem statement and objectives of my project, focusing on the analysis of purchase data from an online store specializing in home appliances and electronics from April 2020 to November 2020.

_**1.1 Problem Statement**_

My primary objective is to analyze the purchase performance of the electronics online store over the specified time period. This involves understanding customer behavior, identifying trends, and uncovering insights that can drive strategic decisions to enhance business performance.

**_1.2 Objectives_**
- Performance Evaluation: Assess the overall performance of the electronics online store, including sales trends, popular products, and customer engagement metrics.
- Customer Segmentation: Utilize RFM (Recency, Frequency, Monetary) analysis to segment customers based on their purchasing behavior and identify distinct customer segments for targeted marketing strategies.
- Dashboard Creation: Develop a user-friendly dashboard to visualize key metrics and insights derived from the analysis, enabling stakeholders to monitor and track the performance of the online store effectively.
  
_**1.3 Key Questions**_

To guide my analysis and address the objectives outlined above, I will explore the following key questions:
- What are the sales trends over the specified time period?
- What are the most popular categories?
- How frequently do customers make purchases, and what is the distribution of purchase amounts?
- How can I segment customers based on their recency, frequency, and monetary value?
  

**2. Dataset Overview**

In this section, I will provide an overview of the purchase data obtained from an electronics online store, covering the period from April 2020 to November 2020. The dataset is sourced from Kaggle ([LINK](https://www.kaggle.com/datasets/mkechinov/ecommerce-purchase-history-from-electronics-store?rvi=1)) and can be found in the folder named _**data**_ with the filename ecommerce_electric_data.csv.

The dataset contains information about purchases made on the electronics online store, including:
- event_time: When event is was happened
- order_id: Order ID
- product_id: Product ID
- category_id: Product category ID
- category_code: Category meaningful name, including category, subcategory and product name information (if present)
- brand: Brand name in lower case (if present)
- price: Product price
- user_id: User ID

**3. Data Pipeline**

In this section, I will outline the data pipeline for ingesting, processing, and analyzing the purchase data from the electronics online store. The pipeline consists of three main stages: ETL data to Data Lake (GCS), ETL data to Data Warehouse (Big Query), and RFM model execution to Segment customers.

![image](https://github.com/nguyennhu0805/DE_ZoomCamp_2024_Final_Project/assets/104962044/3ec28ef5-56ef-4836-9c47-7647def8746b)


_**3.1. ETL to Data Lake (GCS)**_

File: code/
- Extract data from local file (ecommerce_electric_data.csv)
- Transfrom data:
  * Add event_date column to save data in multiple files
  * Refine data type
  * Format the id from scientific notation (e+18) to text
- Load data to GCS (gs://{bucket_name}/{data_name}/{date_str}.csv)

_**3.2. ETL data from Data Lake (GCS) to Data Warehouse (Big Query)**_

File: code/
- Extract data from GCS
- Transform data:
  * Refine data type
  * Format the id from scientific notation (e+18) to text
  * Clean data: remove null price value, replace blank value in columns category_id, category_code, brand to "others"
  * Get category, subcategory and product name information by splitting category_code columns (Ex: category_code electronics.audio.headphone => category electronics, subcategory audio, product_name headphone)
- Load data to Big Query (table ecommerce_electric_data)

_**3.3. Run RFM model**_

File: code/
- Remove blank user_id record to apply RFM model
- Calculate RFM score
- Segment customers by RFM score
![image](https://steps.tn/wp-content/uploads/2022/01/rfm-768x598.png)

**4. Dashboard**

Link: https://lookerstudio.google.com/reporting/751c63b5-2e06-41f5-bef4-c2ee1359f2b9 


   
