# Balancing Skewed Data Across Multiple SSDs
This project aims to provide a strategy to balance data blocks across multiple SSDs so that parallel access to them will be done in the most efficient way. 
Because of data skew, some blocks have higher probability to be accessed, while the other have lower. A simple idea is to put "hotter" blocks on separate devices so that they can be accessed in parallel. 
We want to elaborate on this and search for more advanced strategies.
