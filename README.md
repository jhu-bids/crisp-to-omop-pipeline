# crisp-to-omop-pipeline
* Ingest state of maryland HIE data from CRISP and provide access to de-identified CRISP COVID datasets in OMOP CDM v.5.3.1 format to internal and external researcher project group with approved IRB in place.
## requirements  -
* Each research group has a dedicated workspace and AAD group
* Automate projection of OMOP database from Dev to Prod workspace and set up metastore
* Onboarding of new research groups with databricks workspace
* Data must be de-identified and exist in separate Azure subscription ( CRISP project space with mounted storage and decoupled compute resource) 
* Allow researchers to schedule workflows and install external libraries, provision storage for required persistent datasets for analysis. 

Dr. Chute's lab members responsible for ingesting and harmonizing CRISP data to OMOP instance for research. Stephanie Hong, Richard Zhu Tanner Zhang, Yvette Chen, James Cavallon. Following rotating undergraduate and graduate students also contributed to this work. Eric Kim, Anas Belouali,
and Haeun Lee. 

 


