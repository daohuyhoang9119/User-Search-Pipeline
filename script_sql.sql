CREATE TABLE DimSearch (
    search_id SERIAL PRIMARY KEY,
    most_search_t6 VARCHAR,
    category_t6 VARCHAR,
    trending_type_t6 VARCHAR,
    most_search_t7 VARCHAR,
    category_t7 VARCHAR,
    trending_type_t7 VARCHAR
);

CREATE TABLE FactUserSearch (
    user_id VARCHAR,
    search_id INT,
    FOREIGN KEY (search_id) REFERENCES DimSearch(search_id)
);

-- Query to Find the Most Searched Keyword in Month 6 and its Category
SELECT 
    ds.most_search_t6 AS Most_Searched_Keyword_T6,
    ds.category_t6 AS Category_T6
FROM 
    DimSearch ds
JOIN 
    FactUserSearch fus ON ds.search_id = fus.search_id
GROUP BY 
    ds.most_search_t6, ds.category_t6
ORDER BY 
    COUNT(fus.user_id) DESC
LIMIT 1;