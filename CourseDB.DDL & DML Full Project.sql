
-- DDL --

drop database CourseDB
create database CourseDB
on primary
(Name= 'CourseDB_Data_1',
filename= 'E:\1277544-Projects\SQL project\CourseDB_Data_1.mdf',
size= 25mb,
maxsize= 100mb,
filegrowth= 5% )

log on
(Name= 'CourseDB_Log_1',
filename= 'E:\1277544-Projects\SQL project\CourseDB_Log_1.ldf',
size= 25mb,
maxsize= 100mb,
filegrowth= 5% );

use CourseDB

create table traineer
(trainnerid int primary key,
trainnername varchar (50) );
--drop table traineer

create table courses
(courseid int primary key,
coursename varchar (50) );

create table tsp
(tspid int primary key,
tspname varchar (50) );

create table batch
( batchid int primary key,
batchname varchar (50) ,
trainnerid int references traineer(trainnerid),
courseid int references courses(courseid),
tspid  int references tsp(tspid ),
startdate date,
enddate date,
Status varchar (50) );
--drop table batch

-- DML --


insert into traineer(trainnerid, trainnername) values
(1, 'Nishat Sharmin' ),
(2, 'Azman Ali' ),
(3, 'Ahsan Habib' );

insert into courses(courseid ,coursename) values
(1, 'C#' ),
(2, 'Oracle'),
(3, 'Web development' );

insert into tsp(tspid, tspname ) values
(1, 'Star Computer systems Ltd'),
(2, 'US Software Ltd' ),
(3, ' People & Tech Ltd' );

insert into batch 
(batchid, batchname,trainnerid,courseid,tspid, startdate, enddate, Status ) values
(1, 'R-55', 1,1,1,  '2023-04-03', '2023-07-19', 'Running' ),
(2, 'R-53', 2,2,2, '2021-04-12','2021-04-21','Completed' ),
(3, 'R-52', 3,3,3, '2021-04-07', '2021-04-20', 'Completed' );

select * from traineer
select * from courses
select * from tsp
select * from batch


-- Remove Traineer Information --
select * into trainers from traineer
select * from trainers
delete  trainers where trainnerid= 2

drop table trainers

-- Update Course Name --
Update courses set coursename= 'Microsoft C#.NET' where coursename= 'C#'



--  Remove tspname  --
Alter table tsp
drop column tspname


-- Getting course by specific trainner - Joining --
select traineer.trainnername, courses.coursename, tsp.tspname from batch
join traineer on traineer. trainnerid= batch. trainnerid
join courses on courses.courseid= batch.batchid
join tsp on tsp.tspid= batch.tspid
group by trainnername,coursename,tspname
having trainnername = 'Nishat Sharmin'


--  getting course by specific subject - subquary  --
select * from batch 
where batchname in ( select batchname from batch 
where batchname = 'R-55' )


--  Course Return Running Batch --
select enddate,   iif (enddate < '2023-07-19' , 'No' , 'Yes') as running from batch

-- attach batchName as batchName - isnull,coalesce --
select batchid,batchname,isnull(batchname,'C#') as batchName from batch
select batchid,batchname,coalesce(batchname,'C#') as batchName from batch


-- which courses are available- Store Procedure --
go
create proc availablecourses
as
begin

 select batchname, Status from batch

end

execute availablecourses
 

-- backup Courses information- Trigger --
create table coursestbl
(courseid int primary key,
coursename varchar (50) );

create table backtblcourses
(courseid int primary key,
coursename varchar (50) );

go
create trigger tr_courses on coursestbl
after insert, update
as
insert into backtblcourses
(courseid ,coursename )
select 
i. courseid, i.coursename from inserted i;

go 
insert into coursestbl ( courseid, coursename ) values 
(1, 'Programming, C#' ),
(2, 'Database, Oracle'),
(3, 'Web development, php' );

select * from coursestbl
select * from backtblcourses

-- which course started in which date - table value function -- 
go
create function startingdate()
returns table
return
( select batchname, startdate from batch )
go
select * from  startingdate() 

-- total range of titles table information - scaler valued funtion --
go 
create function scalerinfo()
returns int
begin
declare @L int
select @L= count (*)from batch
return @L
end
go
select dbo.scalerinfo()

-- updated tspid number from courses - multi state function --
go
create function tspIdnumberupdate()
returns @tsptable
table ( traineerid int, courseid int, tspid int )
begin insert into @tsptable ( traineerid, courseid,tspid )
select trainnerid, courseid, tspid+3 
from batch
return
end
go
select * from dbo.tspIdnumberupdate()


--  view of traineers information - create view with encryption--
go
create view vw_traineer
with encryption
as
select * from traineer

select * from vw_traineer

--  view of courses information - create view with schemabinding --
create view vw_courses
with schemabinding
as
select coursename from dbo.courses

select * from vw_courses

--  view of batch information - create view with enryption schemabinding together --
go
create view vw_batchs
with encryption, schemabinding
as
select batchid, batchname from dbo.batch

select * from vw_batchs

-- cte --
go
with cte_tsp as
(select tspid, tspname from tsp )
select * from cte_tsp


------------

-- who didnt pay for courses in time as finepayment- Case --
select batchid,enddate,
case
when enddate >'2021-04-20' then 20

else 0
end as finepayment from batch

--rank --
select batchname,row_number() over (partition by batchname order by batchid) as numberofrow 
from batch
select batchname,rank() over (partition by batchname order by batchid) as NewColumn 
from batch
select batchname,dense_rank() over (partition by batchname order by batchid) as NewColumn 
from batch
select batchname,ntile(4)over (partition by batchname order by batchid) as NewColumn 
from batch

-- Analytical Functiuon --
select batchname,first_value(batchname) over (partition by batchname order by batchid) as new from batch
select batchname,last_value(batchname) over (partition by batchname order by batchid) as new  from batch
select batchname,lag(batchname) over (partition by batchname order by batchid) as new from batch
select batchname,lead(batchname) over (partition by batchname order by batchid) as new  from batch
select batchname,percent_rank() over (partition by batchname order by batchid) as new from batch
select batchname,cume_dist() over (partition by batchname order by batchid) as new  from batch
select batchname,percentile_cont(0.5) within group (order by batchid) over(partition by batchname) as new from batch
select batchname,percentile_disc(0.5) within group (order by batchid) over(partition by batchname) as new from batch


-- non clustered index in batchid --
create nonclustered index in_batchs
on batch(batchid)
















































