-- CREATE TABLE emp_demo (
--     emp_id NUMBER,
--     emp_name VARCHAR2(20),
--     dept_id NUMBER,
--     salary NUMBER
-- );

-- INSERT INTO emp_demo VALUES (1, 'A', 10, 5000);
-- INSERT INTO emp_demo VALUES (2, 'B', 10, 6000);
-- INSERT INTO emp_demo VALUES (3, 'C', 10, 7000);
-- INSERT INTO emp_demo VALUES (4, 'D', 20, 4000);
-- INSERT INTO emp_demo VALUES (5, 'E', 20, 4500);
-- INSERT INTO emp_demo VALUES (6, 'F', 20, 5500);

-- -- select * from emp_demo;
-- ### lead , lag 
-- ### lead , lag with 2
-- ### partition by lead lag 
-- ### partition by lead lag 2 


-- SELECT emp_name, dept_id,salary,
--        lead(salary,2) OVER (partition by dept_id 
--        ORDER BY emp_id) AS next_salary
-- FROM emp_demo;

-- SELECT emp_name, dept_id,salary,
--        lag(salary,2) OVER (partition by dept_id 
--        ORDER BY emp_id) AS prev_salary
-- FROM emp_demo;

-- SELECT emp_name, dept_id,salary,
--        lead(salary,2) OVER (partition by dept_id 
--        ORDER BY emp_id) AS next_salary
-- FROM emp_demo;