-- 1. ROW_NUMBER - Top employee per department
-- select * from (
--     select emp_name,emp_id,dept_id,salary,
--     row_number()over(partition by dept_id order by salary desc) as rank 
--     from emp_analytics 
-- )where rank=1;


-- 2. RANK - Top 2 employees per department (handles ties)
-- select * from (
--     select emp_name,emp_id,dept_id,
--     rank()over(partition by dept_id order by salary desc)as rnk 
--     from emp_analytics
-- )where rnk<=2;

-- 3. DENSE_RANK - Unique ranking per department

-- select emp_name,dept_id,salary,
--         dense_rank()over(partition by dept_id order by salary desc) as drnk
-- from emp_analytics;

-- 4. LEAD - Next employee salary within same department
-- select emp_name,emp_id,dept_id,salary,
--         lead(salary)over(partition by dept_id order by salary desc)as next_salary
-- from emp_analytics;

-- 5. LAG - Previous employee salary within same department
-- select emp_name,emp_id,dept_id,salary,
--         lag(salary)over(partition by dept_id order by salary desc)as prev_salary
-- from emp_analytics;


-- 6. Salary difference with previous employee (real-time scenario)
-- select emp_name,emp_id,dept_id,salary,
-- salary-lag(salary)over(partition by dept_id order by salary desc)as sal_diff
-- from emp_analytics;


-- 7. Compare current salary with next salary
-- select emp_name,emp_id,dept_id,salary,
--         lead(salary)over(partition by dept_id order by salary desc)-salary as next_salary_compare
-- from emp_analytics;

-- 8. Find lowest salary employee per department
-- select * from (
--     select emp_name,emp_id,dept_id,salary,
--     row_number()over(partition by dept_id order by salary asc)as low_salary
--     from emp_analytics
-- )where low_salary=1;


-- 9. Employees earning more than previous employee

-- select emp_name,emp_id,dept_id,salary,
--     lag(salary)over(partition by dept_id order by salary desc)as emp_earn_more
--     from emp_analytics 
-- where salary>lag(salary)over(partition by dept_id order by salary);


-- 10. Second highest salary per department
-- select * from (
--     select emp_name,emp_id,dept_id,salary,
--     row_number()over(partition by dept_id order by salary)as rnk
--     from emp_analytics
-- )where rnk=2;