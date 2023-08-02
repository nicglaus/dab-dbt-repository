  SELECT
    ### Key ###
    CONCAT(model,"_",color,"_",IFNULL(size,"no-size")) AS product_id 
    ###########
    ,model
    ,color
    ,size
    -- name
    ,model_name
    ,color_name
    ,CONCAT(model_name," ",color_name,IF(size IS NULL,"",CONCAT(" - Taille ",size))) AS product_name
    -- product info --
    ,t.new AS pdt_new
    -- stock metrics --
    ,forecast_stock
    ,stock
    -- value
    ,price
  FROM `raw_data_circle.raw_cc_stock` t
