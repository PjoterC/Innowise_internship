-- Output the number of movies in each category, sorted descending.
SELECT c.category_id, COUNT(c.film_id) as movie_num FROM film_category c
GROUP BY c.category_id
ORDER BY movie_num DESC;


-- Output the 10 actors whose movies rented the most, sorted in descending order.
WITH most_rented as(
    SELECT a.actor_id, COUNT(r.rental_id) as rented FROM film_actor a
    JOIN film f ON a.film_id = f.film_id
    JOIN inventory i ON f.film_id = i.film_id
    JOIN rental r ON i.inventory_id = r.inventory_id
    GROUP BY a.actor_id
    ORDER BY rented DESC
    LIMIT 10
    
)
SELECT a.actor_id, a.first_name, a.last_name, m.rented FROM actor a
JOIN most_rented m on a.actor_id = m.actor_id
ORDER BY m.rented DESC;



-- Output the category of movies on which the most money was spent.
WITH payments as(
    SELECT c.category_id, SUM(p.amount) as total FROM film_category c
    JOIN film f ON c.film_id = f.film_id
    JOIN inventory i ON f.film_id = i.film_id
    JOIN rental r ON i.inventory_id = r.inventory_id
    JOIN payment p ON r.rental_id = p.rental_id
    GROUP BY c.category_id
    
)
SELECT category_id, total FROM payments
WHERE total = (SELECT MAX(total) FROM payments);


--Print the names of movies that are not in the inventory. Write a query without using the IN operator.
SELECT f.title FROM film f
LEFT JOIN inventory i ON f.film_id = i.film_id
WHERE i.inventory_id IS NULL;


--Output the top 3 actors who have appeared the most in movies in the “Children” category. If several actors have the same number of movies, output all of them.
WITH appearances AS(
    SELECT a.actor_id, COUNT(a.actor_id) as movie_amount FROM film_actor a
    JOIN film f ON a.film_id = f.film_id
    JOIN film_category fc ON f.film_id = fc.film_id
    JOIN category c ON fc.category_id = c.category_id
    WHERE c.name = 'Children'
    GROUP BY a.actor_id

)
SELECT a.*, ap.movie_amount as children_movie_amount FROM actor a
JOIN appearances ap ON a.actor_id = ap.actor_id
WHERE movie_amount IN (
    SELECT DISTINCT movie_amount FROM appearances
    ORDER BY movie_amount DESC
    LIMIT 3
    );


--Output cities with the number of active and inactive customers 
--(active - customer.active = 1). Sort by the number of inactive customers in descending order.
SELECT c.city, 
SUM(CASE WHEN cus.activebool THEN 1 ELSE 0 END) AS active_customers, 
SUM(CASE WHEN NOT cus.activebool THEN 1 ELSE 0 END) AS inactive_customers
FROM city c
JOIN address a ON c.city_id = a.city_id
JOIN customer cus ON a.address_id = cus.address_id
GROUP BY c.city_id, c.city
ORDER BY inactive_customers DESC;


--Output the category of movies that have the highest number of total rental hours in the city 
--(customer.address_id in this city) and that start with the letter “a”. 
--Do the same for cities that have a “-” in them. Write everything in one query.
(
    SELECT 'starts with a' AS filter, ca.name AS category,
        SUM(EXTRACT(EPOCH FROM (r.return_date - r.rental_date)) / 3600) AS rental_hours
    FROM category ca
    JOIN film_category fc ON ca.category_id = fc.category_id
    JOIN film f  ON fc.film_id = f.film_id
    JOIN inventory i ON f.film_id = i.film_id
    JOIN rental r ON i.inventory_id = r.inventory_id
    JOIN customer cus ON r.customer_id  = cus.customer_id
    JOIN address a ON cus.address_id = a.address_id
    JOIN city ci ON a.city_id = ci.city_id
    WHERE ci.city ILIKE 'a%'
    GROUP BY ca.name
    ORDER BY rental_hours DESC
    LIMIT 1
)
UNION ALL
(
    SELECT 'contains a dash' AS filter, ca.name  AS category, 
        SUM(EXTRACT(EPOCH FROM (r.return_date - r.rental_date)) / 3600) AS rental_hours
    FROM category ca
    JOIN film_category fc ON ca.category_id = fc.category_id
    JOIN film f ON fc.film_id = f.film_id
    JOIN inventory i ON f.film_id = i.film_id
    JOIN rental r ON i.inventory_id = r.inventory_id
    JOIN customer cus ON r.customer_id  = cus.customer_id
    JOIN address a ON cus.address_id = a.address_id
    JOIN city ci ON a.city_id = ci.city_id
    WHERE ci.city LIKE '%-%'
    GROUP BY ca.name
    ORDER BY rental_hours DESC
    LIMIT 1
);