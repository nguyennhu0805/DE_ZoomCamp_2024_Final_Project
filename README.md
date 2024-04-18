# DE_ZoomCamp_2024_Final_Project
**1. Problem Define**
In this section, I will outline the problem statement and objectives of my project, focusing on the analysis of purchase data from an online store specializing in home appliances and electronics from April 2020 to November 2020.

1.1 Problem Statement
Our primary objective is to analyze the purchase performance of the electronics online store over the specified time period. This involves understanding customer behavior, identifying trends, and uncovering insights that can drive strategic decisions to enhance business performance.

1.2 Objectives
- Performance Evaluation: Assess the overall performance of the electronics online store, including sales trends, popular products, and customer engagement metrics.
- Customer Segmentation: Utilize RFM (Recency, Frequency, Monetary) analysis to segment customers based on their purchasing behavior and identify distinct customer segments for targeted marketing strategies.
- Dashboard Creation: Develop a user-friendly dashboard to visualize key metrics and insights derived from the analysis, enabling stakeholders to monitor and track the performance of the online store effectively.
  
1.3 Key Questions
To guide our analysis and address the objectives outlined above, we will explore the following key questions:
- What are the sales trends over the specified time period?
- What are the most popular categories?
- How frequently do customers make purchases, and what is the distribution of purchase amounts?
- How can we segment customers based on their recency, frequency, and monetary value?
  

**2. Dataset Overview**
In this section, we will provide an overview of the purchase data obtained from an electronics online store, covering the period from April 2020 to November 2020. The dataset is sourced from Kaggle ([LINK](https://www.kaggle.com/datasets/mkechinov/ecommerce-purchase-history-from-electronics-store?rvi=1)) and can be found in the folder named _**data**_ with the filename ecommerce_electric_data.csv.

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

3.1. Extract and Load data to Data Lake 
3.2. ETL data from Data Lake to Data Warehouse
3.3. Run RFM model

4. Dashboard
   
