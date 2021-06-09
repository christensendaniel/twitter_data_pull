-- Create a new database called 'Twitter_Data'
-- Connect to the 'master' database to run this snippet
USE master
GO
-- Create the new database if it does not exist already
IF NOT EXISTS (
    SELECT [name]
        FROM sys.databases
        WHERE [name] = N'Twitter_Data'
)
CREATE DATABASE Twitter_Data
GO

CREATE SCHEMA Twitter

USE Twitter_Data
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Twitter].[Twitter](
	[Id] [bigint] NOT NULL,
	[created_at] [datetime] NULL,
	[lang] [nvarchar](max) NULL,
	[tweet] [nvarchar](max) NULL,
	[source] [nvarchar](max) NULL,
	[retweet_count] [int] NULL,
	[reply_count] [int] NULL,
	[like_count] [int] NULL,
	[quote_count] [int] NULL,
	[author_id] [bigint] NULL,
	[created_date] [nvarchar](max) NULL,
	[URL] [nvarchar](max) NULL,
	[clicks] [int] NULL,
	[impressions] [int] NULL,
	[total_engagement] [int] NULL,
	[engagement_percent] [nvarchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
