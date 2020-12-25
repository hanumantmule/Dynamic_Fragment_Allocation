
-- Log Info table at each site

-- Sites which are eligible for 

select afid, asid,count(*) from log_info_s4  group by afid, asid having count(*) >=3 ;

select distinct(ASID) from log_info_s4 where AFID='F6' and ASID != 'S4' ;

select * from  log_info_s4 ; -- and ASID='S3';

select avg(DataVol) from log_info_s1 where AFID = 'F1' ;

select distinct(ASID) from log_info_s1 where AFID= 'F1' and ASID != 'S1';

create table log_info_s1 (AFID varchar(20),ASID varchar(20), ADateTime varchar2(20),RorWAS char(1), DataVol int);

create table log_info_s2 (AFID varchar(20),ASID varchar(20), ADateTime varchar2(20),RorWAS char(1), DataVol int);

create table log_info_s3 (AFID varchar(20),ASID varchar(20), ADateTime varchar2(20),RorWAS char(1), DataVol int);

create table log_info_s4 (AFID varchar(20),ASID varchar(20), ADateTime varchar2(20),RorWAS char(1), DataVol int);

-- At each site maintain Log_Info table
-- AFID means ID of the fragment which is accessed, 
-- ASID means ID of the site which accesses the fragment, 
-- ADateTime means date and time of fragment access from respectively accessing site, 
-- RorWA means read or write of fragment access 
-- DataVol means volume of read data transmitted to and from the accessed fragment or volume of updated data.

select * from log_info;

-- Log_Info at each site sy is written every time when each fragment at owner site is accessed 
-- from different sites sx where y= 1,2,3,... ,S , x= 1,2,3,... ,S, and y = x or x? y.
-- If the ID of local site sy is the same as the ID of the site in the log record Azy (x=y) 
-- that means local access is made, then do nothing. 
-- Otherwise (x?y) that means remote access is made, and then go to the following next process.



create table Employee (Employee_ID varchar2(10),First_Name varchar2(30),Last_Name varchar2(30),Gender char(1),Email varchar2(100),
    Date_of_birth varchar2(20),Date_of_joining varchar2(20),Salary varchar2(15), SSN varchar2(10), Phone_number varchar2(12),
    Place_Name varchar2(50), County varchar2(30),City varchar2(30),
     CONSTRAINT pk_employee PRIMARY KEY(Employee_ID) );
   

select count(*) from Employee;

select count(*) from Employee where salary <= 50000 ; -- 316

select count(*) from Employee where salary > 50000  and salary <= 70000; --615

select count(*) from Employee where salary > 70000  and salary <= 90000; --649

select count(*) from Employee where salary > 90000  and salary <= 110000; --592

select count(*) from Employee where salary > 110000  and salary <= 130000; --622

select count(*) from Employee where salary > 130000  and salary <= 150000; -- 604

select count(*) from Employee where salary > 150000  and salary <= 180000; --953

select count(*) from Employee where salary > 180000 ; -- 615


------------ Store fragmentation details at one table. Fragment - Site mapping 
-- fragment allocation information matrix -----

create table fragment_alloc_site_mapping ( fragment_name varchar2(15), site_name varchar2(10),access_threeshold varchar2(5), time_constraints varchar2(5), CONSTRAINT pk_fragment_alloc_site_mapping PRIMARY KEY(fragment_name));

select * from fragment_alloc_site_mapping;

insert into fragment_alloc_site_mapping values('F1','S1','3','5');

insert into fragment_alloc_site_mapping values('F2','S4','3','5');

insert into fragment_alloc_site_mapping values('F3','S3','3','5');

insert into fragment_alloc_site_mapping values('F4','S2','3','5');

insert into fragment_alloc_site_mapping values('F5','S1','3','5');

insert into fragment_alloc_site_mapping values('F6','S4','3','5');

insert into fragment_alloc_site_mapping values('F7','S2','3','5');

insert into fragment_alloc_site_mapping values('F8','S3','3','5');


--------------------------------------------------------------

insert into log_info_s1 values('F5','S3','07/03/2020 08:16 AM','r','300');

select * from fragment_alloc_site_mapping;


select distinct(fragment_name) from fragment_alloc_site_mapping where site_name='S1';

select distinct(access_threeshold) from fragment_alloc_site_mapping where site_name='S4';

update fragment_alloc_site_mapping set site_name= 'S1' where fragment_name= 'F6';


select ASID, count(*) from log_info_s4 where AFID='F6' and ASID != 'S4' group by ASID having count(*) >= (select distinct(access_threeshold) from fragment_alloc_site_mapping where site_name = 'S4');


