## Project Overview
### Problem:
There are millions of messages exchanged each day between OTA cache partners, and within these request and response messages we have XML data that details information about what's being requested and how many rate and availability changes. Due to the volume of messages and the format in which they are being shared it's hard to get information on how many changes we are sending back to the OTA channels on any given day.

### Solution:
This project was created to collect these XML messages, parse through them for the relevant data points, calculate various new data points from the existing data, and upload that data to GCP tables to support the development of visualizations to provide the business team a clear understanding of both the request and response message volumes.

### Technologies Used:
This project is written in Python utilizing BigQuery, XML ETree, and Pandas. The code is within a GCP sandbox Jupyter notebook and is scheduled to run hourly via a CAWA job.

## Application Flow
1. Read Data from ESB_MSG table
	- A query retrieves data from the ESB_MSG table for 1 day prior and for createdTimestamps that match the current hour.
2. Backfill process
	- Another query is run during each run that looks 3 days back both in terms of the datepart but also the created timestamp. This compares what xact_id and msg_id combinations are missing from the CDS_SUMMARY table. Whatever is missing is then picked up for processing.
		- This was created because there can be cases where the ESB_MSG table is updated later than we expected so this ensures we captured those records.
3. Parse through the XML data
	- Next the code parses through the XML data for each row in the returned query, adding various columns and calculations based on whether it was a REQUEST or RESPONSE type transaction.
	- When all rows have completed it is all put into a data frame object where records with matching MSG_ID and XACT_ID are merged together into a single row to be uploaded to CDS_SUMMARY. Separate data frame objects are created for the REQUEST and RESPONSE tables in GCP.
4. Delete Existing Records
	- To prevent duplicate records from being entered, in case of any overlap there is a delete process as well. This query deletes records from each table using the same filter criteria that was used to get the initial data from ESB_MSG.
5. Uploading to BigQuery
	- Request data is cleaned and then uploaded to the CDS_REQUEST table
	- Response data is cleaned and then uploaded to the CDS_RESPONSE table
	- The merged data frame which combines the information of both Request and Response is uploaded to CDS_SUMMARY.
6. Timestamp updates
	- At this point the previous time that was used for this run is discarded and the current time is stored to become the previous time for the next time this code is executed.
- Optional Functionality:
	- A manual run process was created which allows for custom date filters to be used. This was in case data is missed for a specific date that date can then be entered and processed completely.
