# Data Governance

- All the below steps can ideally be done by a metastore admin.
- Create a group in the Admin console → `DataConsumers`
    
    ![image.png](image.png)
    
- Add users -
    
    ![image.png](image%201.png)
    
- Add members/users in the Group -
    
    ![image.png](image%202.png)
    
- Give this group `User` level permission to the workspace - You can only give admin level or user level. Any one/group who should not be an admin should be given `User` permission to the workspace. So that means that group and all the members of that group now has `User` access on the workspace.
    
    ![image.png](image%203.png)
    
- Now the users under group `DataConsumer` can log into this workspace but they wont have admin access.
- Give `DataConsumer` the below accesses -
    
    ```sql
    GRANT SELECT ON TABLE processing_catalog.schema_harmonized_facts.t_sales_line_items_test TO `DataConsumers`;
    GRANT USAGE ON SCHEMA processing_catalog.schema_harmonized_facts TO `DataConsumers`;
    ```
    
- Verify the same -
    
    ![image.png](image%204.png)
    
- I want to revoke these accesses and only give them very fine tuned access
    
    ![image.png](image%205.png)
    
    ![image.png](image%206.png)
    
- Verify the same -
    
    ![image.png](image%207.png)
    
- I have created views in curated layer for Data Analysis. So I want to give the `DataConsumer` access to those views.
- Grant select access on those views -
    
    ![image.png](image%208.png)
    
- The above access is not enough they should be given USAGE access on the schema to query these views. DO NOT give MANAGE access unless necessary
    
    ![image.png](image%209.png)
    
- Login to the workspace as a member of `DataConsumer` and you’ll see the necessary access
    
    ![image.png](image%2010.png)
    
    ![image.png](image%2011.png)
    
    ![image.png](image%2012.png)
    
- No access to any other schema -
    
    ![image.png](image%2013.png)