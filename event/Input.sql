/* Here before creating of the table need to create database and need to use that database*/
CREATE TABLE EVENTS 
(USERID INT , 
EVENT_TYPE VARCHAR(20),
EVENT_TIME DATETIME);

INSERT INTO EVENTS VALUES (1, 'CLICK', '2023-09-10 09:00:00');
INSERT INTO EVENTS VALUES (1, 'CLICK', '2023-09-10 10:00:00');
INSERT INTO EVENTS VALUES (1, 'SCROLL', '2023-09-10 10:20:00');
INSERT INTO EVENTS VALUES (1, 'CLICK', '2023-09-10 10:50:00');
INSERT INTO EVENTS VALUES (1, 'SCROLL', '2023-09-10 11:40:00');
INSERT INTO EVENTS VALUES (1, 'CLICK', '2023-09-10 12:40:00');
INSERT INTO EVENTS VALUES (1, 'SCROLL', '2023-09-10 12:50:00');
INSERT INTO EVENTS VALUES (2, 'CLICK', '2023-09-10 09:00:00');
INSERT INTO EVENTS VALUES (2, 'SCROLL', '2023-09-10 09:20:00');
INSERT INTO EVENTS VALUES (2, 'CLICK', '2023-09-10 10:30:00');
