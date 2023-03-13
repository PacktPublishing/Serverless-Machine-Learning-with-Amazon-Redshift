CREATE TABLE chapter7_RegressionModel.sporting_event_ticket_info_training (
    ticket_id double precision ,
    event_id bigint,
    sport character varying(500),
    event_date_time timestamp without time zone,
    home_team character varying(500),
    away_team character varying(500),
    location character varying(500),
    city character varying(500),
    seat_level bigint,
    seat_section bigint,
    seat_row character varying(500),
    seat bigint ENCODE az64,
    list_ticket_price double precision,
    final_ticket_price double precision ,
    ticketholder character varying(500)
)
DISTSTYLE AUTO;


--insert ~70% of data into training_set

insert into   chapter7_RegressionModel.sporting_event_ticket_info_training
(  ticket_id , 
    event_id , 
    sport , 
    event_date_time,  
    home_team , 
    away_team , 
    location , 
    city , 
    seat_level,  
    seat_section,  
    seat_row , 
    seat,  
    list_ticket_price,  
    final_ticket_price,  
    ticketholder  
  )
  
 select  
 ticket_id , 
    event_id , 
    sport , 
    event_date_time,  
    home_team , 
    away_team , 
    location , 
    city , 
    seat_level,  
    seat_section,  
    seat_row , 
    seat,  
    list_ticket_price,  
    final_ticket_price,
    ticketholder  
 from chapter7_RegressionModel.sporting_event_ticket_info
 where event_date_time < '2019-10-20';

 
CREATE TABLE chapter7_RegressionModel.sporting_event_ticket_info_validation (
    ticket_id double precision ,
    event_id bigint,
    sport character varying(500),
    event_date_time timestamp without time zone,
    home_team character varying(500),
    away_team character varying(500),
    location character varying(500),
    city character varying(500),
    seat_level bigint,
    seat_section bigint,
    seat_row character varying(500),
    seat bigint ENCODE az64,
    list_ticket_price double precision,
    final_ticket_price double precision ,
    ticketholder character varying(500)
)
DISTSTYLE AUTO;

-- insert ~10% of data into validation set
  
insert into  chapter7_RegressionModel.sporting_event_ticket_info_validation
(  ticket_id , 
    event_id , 
    sport , 
    event_date_time,  
    home_team , 
    away_team , 
    location , 
    city , 
    seat_level,  
    seat_section,  
    seat_row , 
    seat,  
    list_ticket_price,  
    final_ticket_price,  
    ticketholder  
  )
  
 select  
 ticket_id , 
    event_id , 
    sport , 
    event_date_time,  
    home_team , 
    away_team , 
    location , 
    city , 
    seat_level,  
    seat_section,  
    seat_row , 
    seat,  
    list_ticket_price,  
    final_ticket_price,
    ticketholder  
 from chapter7_RegressionModel.sporting_event_ticket_info 
 where event_date_time between '2019-10-20' and '2019-10-22' ;
 
 

CREATE TABLE chapter7_RegressionModel.sporting_event_ticket_info_testing (
    ticket_id double precision ,
    event_id bigint,
    sport character varying(500),
    event_date_time timestamp without time zone,
    home_team character varying(500),
    away_team character varying(500),
    location character varying(500),
    city character varying(500),
    seat_level bigint,
    seat_section bigint,
    seat_row character varying(500),
    seat bigint ENCODE az64,
    list_ticket_price double precision,
    final_ticket_price double precision ,
    ticketholder character varying(500)
)
DISTSTYLE AUTO;

 -- insert ~20% of data into test set
  
insert into   chapter7_RegressionModel.sporting_event_ticket_info_testing
(  ticket_id , 
    event_id , 
    sport , 
    event_date_time,  
    home_team , 
    away_team , 
    location , 
    city , 
    seat_level,  
    seat_section,  
    seat_row , 
    seat,  
    list_ticket_price,  
    ticketholder  
  )
  
 select  
 ticket_id , 
    event_id , 
    sport , 
    event_date_time,  
    home_team , 
    away_team , 
    location , 
    city , 
    seat_level,  
    seat_section,  
    seat_row , 
    seat,  
    list_ticket_price,  
    ticketholder  
 from chapter7_RegressionModel.sporting_event_ticket_info 
 where event_date_time >  '2019-10-22'  
 ;
