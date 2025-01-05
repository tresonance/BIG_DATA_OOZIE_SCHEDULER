 -- [To run this script]: 
 --      mysql -u oozie -p oozie < $HOME/COURSES/BIG-DATA/OOZIE-TEST/purge_oozie_infos.sql
 --      >>>>> password:  Oozie@2024!

 -- The oozie database name is oozie beacause: 
 -- cat ./conf/oozie-site.xml | grep oozie.service.JPAService.jdbc.url -A 3
--        <name>oozie.service.JPAService.jdbc.url</name>
--        <value>jdbc:mysql://localhost:3306/oozie</value>
--        <description>JDBC URL for the Oozie database</description>
--    </property>

--        <name>oozie.service.JPAService.jdbc.url</name>
--        <value>jdbc:mysql://localhost:3306/oozie</value>
--    </property>
--   <!-- Impersonation properties -->


USE oozie;

-- Step 1: Drop the table if it already exists
DROP TABLE IF EXISTS my_print_table;

-- Step 2: Create the table with one column to store the text
CREATE TABLE my_print_table (
    content VARCHAR(255)
);

-- Step 3: Insert the row with the value "-------------------------"
INSERT INTO my_print_table (content) VALUES ('-------------------------');


-- [3]: Vérifiez vontenus des tables  que les tables  :


-- SELECT * FROM WF_JOBS;
-- SSELECT  * FROM my_print_table;
-- SSELECT * FROM WF_ACTIONS;
-- SSELECT  * FROM my_print_table;
-- SSELECT * FROM COORD_JOBS;
-- SSELECT  * FROM my_print_table;
-- SSELECT * FROM COORD_ACTIONS;
-- SSELECT  * FROM my_print_table;
-- SSELECT * FROM BUNDLE_JOBS;


-- [4]: 
SELECT   '-------------------------' FROM my_print_table;
DELETE FROM WF_JOBS;
DELETE FROM WF_ACTIONS;
DELETE FROM COORD_JOBS;
DELETE FROM COORD_ACTIONS;
DELETE FROM BUNDLE_JOBS;

DELETE FROM WF_JOBS WHERE start_time < NOW() - INTERVAL 1 DAY;
DELETE FROM COORD_JOBS WHERE start_time < NOW() - INTERVAL 1 DAY;
DELETE FROM BUNDLE_JOBS WHERE start_time < NOW() - INTERVAL 1 DAY;

-- [5]:VERIFICATION DE LA PURGE 
SELECT  '-------------------------' FROM my_print_table;
SELECT "SELECT * FROM WF_JOBS;" FROM my_print_table;
SELECT * FROM WF_JOBS;
SELECT "SELECT * FROM WF_JOBS;" FROM my_print_table;
SELECT * FROM WF_ACTIONS;
SELECT "SELECT * FROM WF_ACTIONS;" FROM my_print_table;
SELECT * FROM COORD_JOBS;
SELECT "SELECT * FROM COORD_JOBS;" FROM my_print_table;
SELECT * FROM COORD_ACTIONS;
SELECT "SELECT * FROM COORD_ACTIONS;" FROM my_print_table;
SELECT * FROM BUNDLE_JOBS;