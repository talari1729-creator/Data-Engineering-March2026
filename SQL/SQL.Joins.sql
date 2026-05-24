-- CREATE TABLE departments (
--     dept_id      NUMBER PRIMARY KEY,
--     dept_name    VARCHAR2(50) NOT NULL,
--     location     VARCHAR2(50)
-- );
-- INSERT INTO departments VALUES (10, 'Sales',      'Mumbai');
-- INSERT INTO departments VALUES (20, 'Engineering','Bengaluru');
-- INSERT INTO departments VALUES (30, 'HR',         'Delhi');
-- INSERT INTO departments VALUES (40, 'Finance',    'Pune');
-- INSERT INTO departments VALUES (50, 'Legal',      'Chennai');

-- CREATE TABLE employees (
--     emp_id        NUMBER PRIMARY KEY,
--     emp_name      VARCHAR2(50) NOT NULL,
--     dept_id       NUMBER,
--     manager_id    NUMBER,
--     salary        NUMBER(10,2),
--     hire_date     DATE,
--     CONSTRAINT fk_emp_dept
--         FOREIGN KEY (dept_id)
--         REFERENCES departments(dept_id)
-- );

-- INSERT INTO employees VALUES (101, 'Asha',   10, 900, 60000, DATE '2022-01-10');
-- INSERT INTO employees VALUES (102, 'Bharat', 20, 901, 80000, DATE '2021-03-15');
-- INSERT INTO employees VALUES (103, 'Charu',  20, 901, 75000, DATE '2023-07-01');
-- INSERT INTO employees VALUES (104, 'Deep',   20, NULL, NULL, DATE '2024-02-20');
-- INSERT INTO employees VALUES (105, 'Esha',   30, 902, 50000, DATE '2020-11-05');
-- INSERT INTO employees VALUES (106, 'Farhan', NULL, 903, 45000, DATE '2024-01-12'); -- NULL dept
-- INSERT INTO employees VALUES (107, 'Gauri',  40, 904, 90000, DATE '2019-08-25');
-- INSERT INTO employees VALUES (108, 'Hari',   NULL, NULL, NULL, DATE '2025-01-01'); -- NULL dept + NULL salary

-- select * from departments;
-- select * from employees;
#######inner join
-- SELECT e.emp_id, e.emp_name, d.dept_name
-- FROM employees e
-- INNER JOIN departments d
--     ON e.dept_id = d.dept_id;

Example 2: LEFT OUTER JOIN (all employees)
-- SELECT e.emp_id, e.emp_name, d.dept_name
-- FROM employees e
-- LEFT JOIN departments d
--     ON e.dept_id = d.dept_id
-- ORDER BY e.emp_id;

Example 3: RIGHT OUTER JOIN (all departments)
-- SELECT d.dept_id, d.dept_name, e.emp_name
-- FROM employees e
-- RIGHT JOIN departments d
--     ON e.dept_id = d.dept_id
-- ORDER BY d.dept_id, e.emp_name;

Example 4: FULL OUTER JOIN
-- SELECT e.emp_name, d.dept_name
-- FROM employees e
-- FULL OUTER JOIN departments d
--     ON e.dept_id = d.dept_id
-- ORDER BY d.dept_name, e.emp_name;

###left join
-- SELECT e.emp_id, e.emp_name, e.dept_id
-- FROM employees e
-- LEFT JOIN departments d
--     ON e.dept_id = d.dept_id;

Employees with no matching department
-- SELECT e.emp_id, e.emp_name, e.dept_id
-- FROM employees e
-- LEFT JOIN departments d
--     ON e.dept_id = d.dept_id
-- WHERE d.dept_id IS NULL;

Departments with no employees
-- SELECT d.dept_id, d.dept_name
-- FROM departments d
-- LEFT JOIN employees e
--     ON d.dept_id = e.dept_id
-- WHERE e.emp_id IS NULL;

INNER JOIN with filter on location
-- SELECT e.emp_name, d.dept_name, d.location
-- FROM employees e
-- JOIN departments d
--     ON e.dept_id = d.dept_id
-- WHERE d.location = 'Bengaluru';


Example 8: Count employees per department (including empty departments)
-- SELECT d.dept_id,
--        d.dept_name,
--        COUNT(e.emp_id) AS employee_count
-- FROM departments d
-- LEFT JOIN employees e
--     ON d.dept_id = e.dept_id
-- GROUP BY d.dept_id, d.dept_name
-- ORDER BY d.dept_id;

Example 9: Average salary per department
-- SELECT d.dept_name,
--        ROUND(AVG(e.salary), 2) AS avg_salary
-- FROM departments d
-- LEFT JOIN employees e
--     ON d.dept_id = e.dept_id
-- GROUP BY d.dept_name
-- ORDER BY d.dept_name;

Example 10: Show 'No Department' for NULL joins
-- SELECT e.emp_name,
--        NVL(d.dept_name, 'No Department') AS dept_label
-- FROM employees e
-- LEFT JOIN departments d
--     ON e.dept_id = d.dept_id
-- ORDER BY e.emp_id;


Example 11: Employees hired after 2023 with department names
-- SELECT e.emp_name, e.hire_date, d.dept_name
-- FROM employees e
-- LEFT JOIN departments d
--     ON e.dept_id = d.dept_id
-- WHERE e.hire_date >= DATE '2023-01-01'
-- ORDER BY e.hire_date;

Example 12: Departments having at least 2 employees
-- SELECT d.dept_name, COUNT(*) AS emp_count
-- FROM departments d
-- JOIN employees e
--     ON d.dept_id = e.dept_id
-- GROUP BY d.dept_name
-- HAVING COUNT(*) >= 2;

Example 13: Employee count + total salary by department
-- SELECT d.dept_name,
--        COUNT(e.emp_id) AS emp_count,
--        SUM(e.salary) AS total_salary
-- FROM departments d
-- LEFT JOIN employees e
--     ON d.dept_id = e.dept_id
-- GROUP BY d.dept_name
-- ORDER BY d.dept_name;

Example 14: Use COALESCE for salary display
-- SELECT e.emp_name,
--        COALESCE(TO_CHAR(e.salary), 'Salary Missing') AS salary_text,
--        d.dept_name
-- FROM employees e
-- LEFT JOIN departments d
--     ON e.dept_id = d.dept_id;

Example 15: Find employees in departments located in Mumbai or Delhi
-- SELECT e.emp_name, d.dept_name, d.location
-- FROM employees e
-- JOIN departments d
--     ON e.dept_id = d.dept_id
-- WHERE d.location IN ('Mumbai', 'Delhi');

Example 16: Sort by department then highest salary
-- SELECT d.dept_name, e.emp_name, e.salary
-- FROM departments d
-- LEFT JOIN employees e
--     ON d.dept_id = e.dept_id
-- ORDER BY d.dept_name, e.salary DESC NULLS LAST;


Example 17: Anti-join style using NOT EXISTS (departments without employees)
-- SELECT d.dept_id, d.dept_name
-- FROM departments d
-- WHERE NOT EXISTS (
--     SELECT 1
--     FROM employees e
--     WHERE e.dept_id = d.dept_id
-- );

Example 18: Join with computed bucket (high/medium/low salary)
-- SELECT e.emp_name,
--        d.dept_name,
--        CASE
--            WHEN e.salary >= 80000 THEN 'High'
--            WHEN e.salary >= 50000 THEN 'Medium'
--            WHEN e.salary IS NULL THEN 'Unknown'
--            ELSE 'Low'
--        END AS salary_band
-- FROM employees e
-- LEFT JOIN departments d
--     ON e.dept_id = d.dept_id;

Example 19: Count NULL department employees via join result
-- SELECT COUNT(*) AS no_dept_employees
-- FROM employees e
-- LEFT JOIN departments d
--     ON e.dept_id = d.dept_id
-- WHERE d.dept_id IS NULL;

Example 20: CROSS JOIN (all combinations)
-- SELECT d.dept_name, e.emp_name
-- FROM departments d
-- CROSS JOIN employees e;