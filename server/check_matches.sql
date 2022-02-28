select id, name, address, clean, primary_rest_id 
from 
(ri_restaurants 
JOIN ri_linked 
ON ri_restaurants.id=ri_linked.original_rest_id); 