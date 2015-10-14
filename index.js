
/****

********SunEdison*********

 : Anurag Bhardwaj

Version                     Author                      Change
1.0                     Anurag Bhardwaj             Initial Version


****/

var pjson = require('./package.json');
var region = process.env['AWS_REGION'];

if (!region || region === null || region === "") {
    region = "us-west-2";
    console.log("AWS Lambda Redshift Database Loader using default region " + region);
}

//Requiring aws-sdk. 
var aws = require('aws-sdk');
aws.config.update({
    region : region
});

//Requiring S3 module. 
var s3 = new aws.S3({
    apiVersion : '2006-03-01',
    region : region
});
//Requiring dynamoDB module. 
var dynamoDB = new aws.DynamoDB({
    apiVersion : '2012-08-10',
    region : region
});

//Requiring SNS module. 
var sns = new aws.SNS({
    apiVersion : '2010-03-31',
    region : region
});

//Importing exteral file constants. 
require('./constants');

//Importing kmsCrypto. 
var kmsCrypto = require('./kmsCrypto');
kmsCrypto.setRegion(region);

var common = require('./common');
var async = require('async');
var uuid = require('node-uuid');

//Importing postgre. 
var pg = require('pg');

//Importing https. 
var http = require('https');

var upgrade = require('./upgrades');
var zlib = require('zlib');

//Importing querystring to get more DB calls into the script.
var querystring = require('querystring');
var parseString = require('xml2js').parseString;

var responseString = '';
var test;


var conString = "postgresql://abhardwaj:Master12@sunedisondatawarehouse.cgnr3c8sn1sz.us-west-2.redshift.amazonaws.com:5439/sunedison";

var queryCreateSPTSkeleton = 'CREATE TABLE spt(firstname varchar(200),lastname varchar(200),total_contract_price varchar(100),so_number varchar(100), so_promised_date date, so_status varchar(200), actual_del_date date,so_created_q varchar(100),so_created_ww varchar(100),so_created_date date, lead_create_q varchar(100),lead_create_ww varchar(100),lead_create_date date, so_delivery_date date, kw_ordered varchar(100),kw_system varchar(100),delivery_2_mp2 varchar(100),so_created_2_delivery varchar(100),pda_2_so_create varchar(100),final_completion_certificate varchar(200),inverter_quantity varchar(100),inverter_model_1 varchar(100),inverter_mfr_1 varchar(100),pv_modules_quantity varchar(50),pv_module_model varchar(50),pda_signed_date date,subsidiary_no_hierarchy varchar(200),street_1 varchar(5000),city varchar(100),zip varchar(50),tranche_id varchar(200),site_visit_date date,contract_canceled_date date,fully_executed_date date,assignment_agmnt_update_date date,assignment_agmnt_status varchar(100),commissioned_date date,pda_last_update date,pda_status varchar(200),site_visit_status varchar(100),contract_status_update date,contract_status varchar(100),cc_status_update date,credit_check_status varchar(100),proposal_status_update date,kw_proposed varchar(50),customer_id varchar(50),tsm varchar(200),site_id varchar(50),proposal_status varchar(100),sai_installer varchar(500),stage varchar(100),stage_sai_placeholder varchar(100),stage_sop_placeholder varchar(100),fully_executed_ww varchar(100),contract_2_site_visit varchar(100),contract_2_pda varchar(100),mp2_2_mp3 varchar(100),contract_2_mp2 varchar(100),contract_2_mp3 varchar(100),channel varchar(100),partner varchar(100),mp2_approval_date date,mp3_approval_date date,cc_status_ww varchar(100),cc_status_q varchar(100),contract_status_update_ww varchar(100),contract_status_update_q varchar(100),fully_executed_q varchar(100),site_visit_ww varchar(100),site_visit_q varchar(100),pda_complete_date date,pda_complete_ww varchar(100),pda_complete_q varchar(100),commissioned_ww varchar(100),commissioned_q varchar(100),mp2_apprvd_date date,mp2_apprvd_ww varchar(100),mp2_apprvd_q varchar(100),mp3_apprvd_date date,mp3_apprvd_ww varchar(100),mp3_apprvd_q varchar(100),assignment_agmnt_ww varchar(100),assignment_agmnt_q varchar(100),financing_program varchar(100), permit_status varchar(50), permit_date date, install_status varchar(200), target_install_date date, actual_install_date date, sales_agent varchar(200));';
var queryMakeSPT = "insert into spt select customer.firstname as firstname, customer.lastname as lastname,site.total_contract_price as total_contract_price, sales_order.number as SO_Number, sales_order.current_promise_date as SO_Promised_date, sales_order.orderstatus as SO_Status, sales_order.actual_delivery_date as Actual_Del_Date,CASE WHEN NVL2(sales_order.datecreated, TO_CHAR(sales_order.datecreated, 'YYYY') || '_Q' || TO_CHAR(sales_order.datecreated, 'Q'), Null) IS NOT NULL THEN NVL2(sales_order.datecreated, TO_CHAR(sales_order.datecreated, 'YYYY') || '_Q' || TO_CHAR(sales_order.datecreated, 'Q'), Null) END AS SO_Created_Q,CASE WHEN NVL2(sales_order.datecreated, TO_CHAR(sales_order.datecreated, 'IYYY') || '_' || TO_CHAR(sales_order.datecreated, 'IW'), Null) IS NOT NULL THEN NVL2(sales_order.datecreated, TO_CHAR(sales_order.datecreated, 'IYYY') || '_' || TO_CHAR(sales_order.datecreated, 'IW'), Null) END AS SO_Created_WW, CASE WHEN TO_DATE(sales_order.datecreated,'YYYY-MM-DD') IS NOT NULL THEN TO_DATE(sales_order.datecreated,'YYYY-MM-DD') END AS SO_Created_Date,CASE WHEN NVL2(customer.date_created, TO_CHAR(customer.date_created, 'YYYY') || '_Q' || TO_CHAR(customer.date_created, 'Q'), Null) IS NOT NULL THEN NVL2(customer.date_created, TO_CHAR(customer.date_created, 'YYYY') || '_Q' || TO_CHAR(customer.date_created, 'Q'), Null) END AS Lead_Create_Q,case when NVL2(customer.date_created, TO_CHAR(customer.date_created, 'IYYY') || '_' || TO_CHAR(customer.date_created, 'IW'), Null) is not null then NVL2(customer.date_created, TO_CHAR(customer.date_created, 'IYYY') || '_' || TO_CHAR(customer.date_created, 'IW'), Null) end as Lead_Create_WW, CASE WHEN TO_Date(customer.date_created,'YYYY-MM-DD') is not null then TO_Date(customer.date_created,'YYYY-MM-DD') end as Lead_Create_Date,sales_order.actual_delivery_date as SO_Delivery_Date, sales_order.pv_total_kw_ordered as KW_ordered, CASE WHEN (sales_order.pv_total_kw_ordered NOT LIKE '') THEN sales_order.pv_total_kw_ordered ELSE customer.proposed_system_size_pvkwh END AS KW_System, CASE WHEN (NVL(site.milestone_2_payment_approval_, site.installer2_payment_approval_d) IS NOT NULL AND sales_order.actual_delivery_date IS NOT NULL) THEN TO_DATE(NVL(site.milestone_2_payment_approval_, site.installer2_payment_approval_d),'YYYY-MM-DD') - sales_order.actual_delivery_date ELSE NULL END as Delivery_2_MP2, CASE WHEN (sales_order.datecreated IS NOT NULL AND sales_order.actual_delivery_date IS NOT NULL) THEN sales_order.actual_delivery_date - TO_DATE(sales_order.datecreated,'YYYY-MM-DD') ELSE NULL END AS SO_Created_2_Delivery, CASE WHEN (customer.homeowner_proj_document_sta_id IN ('Signed by Partner', 'Signed by SunEdison') AND sales_order.datecreated IS NOT NULL) THEN TO_DATE(sales_order.datecreated,'YYYY-MM-DD') - customer.proj_definition_doc_last_upda ELSE NULL END as PDA_2_SO_Create, SITE.final_completion_certificate AS final_completion_certificate,SITE.inverter_quantity AS inverter_quantity,SITE.inverter_model_1_id AS inverter_model_1,SITE.inverter_mfr_1_id AS inverter_mfr_1,SITE.pv_modules_quantity AS pv_modules_quantity,SITE.pv_module_model AS pv_module_model,CUSTOMER.PDA_SIGNED_DATE AS PDA_SIGNED_DATE,CUSTOMER.subsidiary_no_hierarchy AS SUBSIDIARY_NO_HIERARCHY,SITE.STREET_1 AS STREET_1,SITE.CITY AS CITY,SITE.ZIP AS ZIP,SITE.TRANCHE_ID AS TRANCHE_ID,SITE.SITE_VISIT_DATE AS SITE_VISIT_DATE,CUSTOMER.contract_canceled_date AS CONTRACT_CANCELED_DATE,CUSTOMER.FULLY_EXECUTED_DATE AS FULLY_EXECUTED_DATE,CUSTOMER.executed_assign_agrmnt_updat AS ASSIGNMENT_AGMNT_UPDATE_DATE,CUSTOMER.executed_assgn_agmnt_status AS ASSIGNMENT_AGMNT_STATUS,SITE.commissioned_date AS COMMISSIONED_DATE,CUSTOMER.proj_definition_doc_last_upda AS PDA_LAST_UPDATE,CUSTOMER.homeowner_proj_document_sta_id AS PDA_STATUS,SITE.site_visit_status_id AS SITE_VISIT_STATUS,CUSTOMER.lease_contract_status_last_up AS CONTRACT_STATUS_UPDATE,CUSTOMER.homeowner_lease_contract_st_id AS CONTRACT_STATUS,CUSTOMER.credit_check_last_update_date AS CC_STATUS_UPDATE,CASE WHEN CUSTOMER.homeowner_credit_check_status NOT LIKE '' THEN CUSTOMER.homeowner_credit_check_status END AS CREDIT_CHECK_STATUS,CASE WHEN CUSTOMER.proposal_status_last_update_date IS NOT NULL THEN CUSTOMER.proposal_status_last_update_date END AS PROPOSAL_STATUS_UPDATE,CASE WHEN CUSTOMER.proposed_system_size_pvkwh NOT LIKE '' THEN CUSTOMER.proposed_system_size_pvkwh END AS KW_PROPOSED,CASE WHEN (CUSTOMER.RECORD NOT LIKE '') THEN CUSTOMER.RECORD END AS customer_id, CASE WHEN (SITE.TSM IS NOT NULL) THEN SITE.TSM END AS TSM, CASE WHEN SITE.SITE_ID IS NOT NULL THEN SITE.SITE_ID END AS SITE_ID, CASE WHEN CUSTOMER.PROPOSAL_STATUS_ID IS NOT NULL THEN CUSTOMER.PROPOSAL_STATUS_ID END AS PROPOSAL_STATUS, CASE WHEN SITE.SAI_INSTALLER IS NOT NULL THEN SITE.SAI_INSTALLER END AS SAI_INSTALLER, CASE WHEN ((CUSTOMER.FULLY_EXECUTED_DATE IS NOT NULL) AND (CUSTOMER.CONTRACT_CANCELED_DATE IS NOT NULL)) THEN 'Contract Cancelled' WHEN SITE.MILESTONE_3_PAYMENT_APPROVAL_ IS NOT NULL THEN 'Pending Dropdown' WHEN SITE.MILESTONE_2_PAYMENT_APPROVAL_ IS NOT NULL THEN 'Pending PTO' WHEN sales_order.actual_delivery_date IS NOT NULL AND customer.financing_program NOT LIKE '' THEN 'Pending Install Complete' WHEN sales_order.number IS NOT NULL AND customer.financing_program NOT LIKE '' THEN 'Pending Delivery' WHEN CUSTOMER.HOMEOWNER_PROJ_DOCUMENT_STA_ID IN ('Signed by Partner', 'Signed by SunEdison') THEN 'Pending So Create' WHEN ((CUSTOMER.FULLY_EXECUTED_DATE IS NOT NULL)  AND (CUSTOMER.CONTRACT_CANCELED_DATE IS NULL)) THEN 'Pending PDA' WHEN CUSTOMER.HOMEOWNER_LEASE_CONTRACT_ST_ID LIKE 'Signed by HO%' THEN 'Pending Contract Completion' WHEN CUSTOMER.PROPOSAL_STATUS_ID IN ('Proposal Accepted', 'Proposal Initiated', 'Proposal Signed') THEN 'Pending Contract' WHEN customer.proposal_status_id LIKE '' AND (customer.financing_program NOT LIKE '' OR customer.purchase_type_id IN ('Lease - Monthly', 'Loan', 'PPA')) THEN 'Pending Proposal' ELSE NULL END AS STAGE, CASE WHEN CUSTOMER.PARTNER_SUB_TYPE_ID = 'SALES ENGINE (Seller)' THEN(CASE WHEN CUSTOMER.HOMEOWNER_PROJ_DOCUMENT_STA_ID= 'Signed by SunEdison' THEN (CASE WHEN SITE.INSTALL_STATUS NOT LIKE '' THEN ('Installation: ' || SITE.INSTALL_STATUS) ELSE 'PDA: Signed by SunEdison' END)   ELSE (CASE WHEN SITE.SITE_VISIT_STATUS_ID = 'Completed' THEN (CASE WHEN CUSTOMER.homeowner_proj_document_sta_id NOT LIKE '' THEN('PDA: ' || CUSTOMER.homeowner_proj_document_sta_id) ELSE 'Site Survey: Completed' END)   ELSE (CASE WHEN SITE.SITE_VISIT_STATUS_ID LIKE '' THEN 'Site Survey: Not Scheduled' ELSE ('Site Survey: ' ||SITE.SITE_VISIT_STATUS_ID) END) END) END) END AS STAGE_SAI_PLACEHOLDER, CASE WHEN (CUSTOMER.FULLY_EXECUTED_DATE IS NOT NULL) AND (CUSTOMER.CONTRACT_CANCELED_DATE IS NOT NULL) THEN 'Contract Cancelled' WHEN SITE.TRANCHE_ID NOT LIKE '' THEN 'Tranche Complete' WHEN SITE.MILESTONE_3_PAYMENT_APPROVAL_ IS NOT NULL THEN 'Pending Dropdown' WHEN SITE.MILESTONE_2_PAYMENT_APPROVAL_ IS NOT NULL THEN 'Pending PTO' WHEN sales_order.actual_delivery_date IS NOT NULL AND customer.financing_program NOT LIKE '' THEN 'Pending Install Complete' WHEN sales_order.number IS NOT NULL AND customer.financing_program NOT LIKE '' THEN 'Pending Delivery' WHEN CUSTOMER.HOMEOWNER_PROJ_DOCUMENT_STA_ID IN ('Signed by Partner', 'Signed by SunEdison') THEN 'Pending So Create' WHEN CUSTOMER.FULLY_EXECUTED_DATE IS NOT NULL AND CUSTOMER.CONTRACT_CANCELED_DATE IS NULL THEN 'Pending PDA' WHEN CUSTOMER.HOMEOWNER_LEASE_CONTRACT_ST_ID LIKE 'Signed by HO%' THEN 'Pending Contract Completion'  WHEN CUSTOMER.PROPOSAL_STATUS_ID IN ('Proposal Accepted', 'Proposal Initiated', 'Proposal Signed') THEN 'Pending Contract' WHEN customer.proposal_status_id LIKE '' AND (customer.financing_program NOT LIKE '' OR customer.purchase_type_id IN ('Lease - Monthly', 'Loan', 'PPA')) THEN 'Pending Proposal' ELSE NULL END AS STAGE_SOP_PLACEHOLDER, NVL2(CUSTOMER.FULLY_EXECUTED_DATE, TO_CHAR(CUSTOMER.FULLY_EXECUTED_DATE, 'IYYY') || '_' || TO_CHAR(CUSTOMER.FULLY_EXECUTED_DATE, 'IW'), NULL) AS FULLY_EXECUTED_WW, CASE WHEN ( SITE.SITE_VISIT_STATUS_ID IN ('Completed')) THEN CUSTOMER.FULLY_EXECUTED_DATE - SITE.SITE_VISIT_DATE ELSE NULL END AS CONTRACT_2_SITE_VISIT, CASE WHEN ( CUSTOMER.HOMEOWNER_PROJ_DOCUMENT_STA_ID IN ('Signed by Partner', 'Signed by SunEdison')) THEN CUSTOMER.PROJ_DEFINITION_DOC_LAST_UPDA - CUSTOMER.FULLY_EXECUTED_DATE ELSE NULL END AS CONTRACT_2_PDA, CASE WHEN (NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_, INSTALLER2_PAYMENT_APPROVAL_D) IS NOT NULL AND NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_,SITE.INSTALLER3_PAYMENT_APPROVAL_D) IS NOT NULL) THEN TO_DATE(NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_,SITE.INSTALLER3_PAYMENT_APPROVAL_D),'YYYY-MM-DD') - TO_DATE(NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D),'YYYY-MM-DD') ELSE NULL END AS MP2_2_MP3, CASE WHEN (NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D) IS NOT NULL) THEN TO_DATE(NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D),'YYYY-MM-DD') - CUSTOMER.FULLY_EXECUTED_DATE ELSE NULL END AS CONTRACT_2_MP2, CASE WHEN (NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_,SITE.INSTALLER3_PAYMENT_APPROVAL_D) IS NOT NULL) THEN TO_DATE(NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_,SITE.INSTALLER3_PAYMENT_APPROVAL_D),'YYYY-MM-DD') - CUSTOMER.FULLY_EXECUTED_DATE ELSE NULL END AS CONTRACT_2_MP3, CASE WHEN (SITE.SALES_CHANNEL_ID NOT LIKE '') THEN SITE.SALES_CHANNEL_ID WHEN CUSTOMER.ASSIGNED_PARTNER_ID LIKE 'SunEdison Inside%' THEN 'IS' WHEN CUSTOMER.ASSIGNED_PARTNER_ID IN ('Brite Energy', 'Complete Solar Solution Inc') THEN 'KP' WHEN CUSTOMER.ASSIGNED_PARTNER_ID IN ('Clear Solar - Sales Engine') THEN 'SE' WHEN CUSTOMER.ASSIGNED_PARTNER_ID IN ('Evolve Solar') THEN 'EV' END AS CHANNEL, CASE WHEN NVL(SITE.PARTNER_ID,CUSTOMER.ASSIGNED_PARTNER_ID) NOT IN ('API-TestPartner_Prod','Test Partner, Inc..') THEN NVL(SITE.PARTNER_ID,CUSTOMER.ASSIGNED_PARTNER_ID) END AS PARTNER, NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D) AS MP2_APPROVAL_DATE, NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_, SITE.INSTALLER3_PAYMENT_APPROVAL_D) AS MP3_APPROVAL_DATE, NVL2(CUSTOMER.CREDIT_CHECK_LAST_UPDATE_DATE, TO_CHAR(CUSTOMER.CREDIT_CHECK_LAST_UPDATE_DATE, 'IYYY') || '_' || TO_CHAR(CUSTOMER.CREDIT_CHECK_LAST_UPDATE_DATE, 'IW'), NULL) AS CC_STATUS_WW, NVL2(CUSTOMER.CREDIT_CHECK_LAST_UPDATE_DATE, TO_CHAR(CUSTOMER.CREDIT_CHECK_LAST_UPDATE_DATE, 'YYYY') || '_Q' || TO_CHAR(CUSTOMER.CREDIT_CHECK_LAST_UPDATE_DATE, 'Q'), NULL) AS CC_STATUS_Q, NVL2(CUSTOMER.LEASE_CONTRACT_STATUS_LAST_UP, TO_CHAR(CUSTOMER.LEASE_CONTRACT_STATUS_LAST_UP, 'IYYY') || '_' || TO_CHAR(CUSTOMER.LEASE_CONTRACT_STATUS_LAST_UP, 'IW'), NULL) AS CONTRACT_STATUS_UPDATE_WW, NVL2(CUSTOMER.LEASE_CONTRACT_STATUS_LAST_UP, TO_CHAR(CUSTOMER.LEASE_CONTRACT_STATUS_LAST_UP, 'YYYY') || '_Q' || TO_CHAR(CUSTOMER.LEASE_CONTRACT_STATUS_LAST_UP, 'Q'), NULL) AS CONTRACT_STATUS_UPDATE_Q, NVL2(CUSTOMER.FULLY_EXECUTED_DATE, TO_CHAR(CUSTOMER.FULLY_EXECUTED_DATE, 'YYYY') || '_Q' || TO_CHAR(CUSTOMER.FULLY_EXECUTED_DATE, 'Q'), NULL) AS FULLY_EXECUTED_Q, NVL2(SITE.SITE_VISIT_DATE, TO_CHAR(SITE.SITE_VISIT_DATE, 'IYYY') || '_' || TO_CHAR(SITE.SITE_VISIT_DATE, 'IW'), NULL) AS SITE_VISIT_WW, NVL2(SITE.SITE_VISIT_DATE, TO_CHAR(SITE.SITE_VISIT_DATE, 'YYYY') || '_Q' || TO_CHAR(SITE.SITE_VISIT_DATE, 'Q'), NULL) AS SITE_VISIT_Q, CASE WHEN (CUSTOMER.HOMEOWNER_PROJ_DOCUMENT_STA_ID IN ('Signed by Partner', 'Signed by SunEdison')) THEN CUSTOMER.PROJ_DEFINITION_DOC_LAST_UPDA ELSE NULL END AS PDA_COMPLETE_DATE, CASE WHEN (CUSTOMER.HOMEOWNER_PROJ_DOCUMENT_STA_ID IN ('Signed by Partner', 'Signed by SunEdison')) THEN TO_CHAR(CUSTOMER.PROJ_DEFINITION_DOC_LAST_UPDA, 'IYYY') || '_' || TO_CHAR(CUSTOMER.PROJ_DEFINITION_DOC_LAST_UPDA, 'IW') ELSE NULL END AS PDA_COMPLETE_WW, CASE WHEN (CUSTOMER.HOMEOWNER_PROJ_DOCUMENT_STA_ID IN ('Signed by Partner', 'Signed by SunEdison')) THEN TO_CHAR(CUSTOMER.PROJ_DEFINITION_DOC_LAST_UPDA, 'YYYY') || '_Q' || TO_CHAR(CUSTOMER.PROJ_DEFINITION_DOC_LAST_UPDA, 'Q') ELSE NULL END AS PDA_COMPLETE_Q, NVL2(SITE.COMMISSIONED_DATE, TO_CHAR(SITE.COMMISSIONED_DATE, 'IYYY') || '_' || TO_CHAR(SITE.COMMISSIONED_DATE, 'IW'), NULL) AS COMMISSIONED_WW, NVL2(SITE.COMMISSIONED_DATE, TO_CHAR(SITE.COMMISSIONED_DATE, 'YYYY') || '_Q' || TO_CHAR(SITE.COMMISSIONED_DATE, 'Q'), NULL) AS COMMISSIONED_Q, NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D) AS MP2_APPRVD_DATE, NVL2(NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D), TO_CHAR(NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D), 'IYYY') || '_' || TO_CHAR(NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D), 'IW'), NULL) AS MP2_APPRVD_WW, NVL2(NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D), TO_CHAR(NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D), 'YYYY') || '_Q' || TO_CHAR(NVL(SITE.MILESTONE_2_PAYMENT_APPROVAL_,SITE.INSTALLER2_PAYMENT_APPROVAL_D), 'Q'), NULL) AS MP2_APPRVD_Q, NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_, SITE.INSTALLER3_PAYMENT_APPROVAL_D) AS MP3_APPRVD_DATE, NVL2(NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_, SITE.INSTALLER3_PAYMENT_APPROVAL_D), TO_CHAR(NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_, SITE.INSTALLER3_PAYMENT_APPROVAL_D), 'IYYY') || '_' || TO_CHAR(NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_, SITE.INSTALLER3_PAYMENT_APPROVAL_D), 'IW'), NULL) AS MP3_APPRVD_WW, NVL2(NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_, SITE.INSTALLER3_PAYMENT_APPROVAL_D), TO_CHAR(NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_, SITE.INSTALLER3_PAYMENT_APPROVAL_D), 'YYYY') || '_Q' || TO_CHAR(NVL(SITE.MILESTONE_3_PAYMENT_APPROVAL_, SITE.INSTALLER3_PAYMENT_APPROVAL_D), 'Q'), NULL) AS MP3_APPRVD_Q,NVL2(CUSTOMER.EXECUTED_ASSIGN_AGRMNT_UPDAT, TO_CHAR(CUSTOMER.EXECUTED_ASSIGN_AGRMNT_UPDAT, 'IYYY') || '_' || TO_CHAR(CUSTOMER.EXECUTED_ASSIGN_AGRMNT_UPDAT, 'IW'), NULL) AS ASSIGNMENT_AGMNT_WW, NVL2(CUSTOMER.EXECUTED_ASSIGN_AGRMNT_UPDAT, TO_CHAR(CUSTOMER.EXECUTED_ASSIGN_AGRMNT_UPDAT, 'YYYY') || '_Q' || TO_CHAR(CUSTOMER.EXECUTED_ASSIGN_AGRMNT_UPDAT, 'Q'), NULL) AS ASSIGNMENT_AGMNT_Q, CASE WHEN (customer.financing_program LIKE 'PP%' AND customer.purchase_type_id LIKE 'Leas%') THEN 'AZLS' ELSE customer.financing_program END AS financing_program, site.permit_status as permit_status, site.actual_permit_date as permit_date, site.install_status as install_status, site.target_install_date as target_install_date, site.install_completed_date as actual_install_date, case when(customer.assigned_partner_id like 'Evolve Solar') then customer.sales_agent else customer.assigned_to_partner_sales_agent end as sales_agent FROM site left join customer on site.homeowner_id = customer.record left join sales_order on NVL((CASE when len(site.nr_interconnected_so) != 0 and len(site.nr_interconnected_so) !=1 then substring(site.nr_interconnected_so, 8, (LEN(site.nr_interconnected_so)-7)) end),(CASE when len(site.salesorder_to_homeowner_id) != 0 and len(site.salesorder_to_homeowner_id) !=1 then substring(site.salesorder_to_homeowner_id, 8, (LEN(site.salesorder_to_homeowner_id)-7)) end)) = sales_order.number where customer.subsidiary_no_hierarchy in ('SERS Operations', 'Team-Solar, Inc.', 'NVT Licenses, LLC', 'NY SunEdison Residential Svcs, LLC', 'LightWing', 'Evolve') and (customer.date_created >= '2014-06-01' or customer.date_created >= '2014-06-01') and (lease_contract_status_last_up >= '2014-11-01' or lease_contract_status_last_up is null) and (site.partner_id not in ('API-TestPartner_Prod','Test Partner, Inc..')) and (customer.assigned_partner_id not in ('API-TestPartner_Prod','Test Partner, Inc..')) and (customer.purchase_type_id not like '') and (customer.firstname not like 'TL%') and (customer.lastname not like 'TL%') and ((site.salesorder_to_homeowner_id not like '' and site.nr_interconnected_so like '') or (site.salesorder_to_homeowner_id like '' and site.nr_interconnected_so not like '') or (site.salesorder_to_homeowner_id not like '' and site.nr_interconnected_so not like '') or (site.salesorder_to_homeowner_id like '' and site.nr_interconnected_so like ''))";
var queryDropSPT = 'DROP TABLE IF EXISTS spt;';

exports.handler = function(event, context) {
	dropSPT();
}

var dropSPT = function(){
	pg.connect(conString, function(err,client){
	    if(err){
	        return console.log("Connection error. ", err);
	    }
	
	    console.log("Connection Established.");
	
	    //Querying redshift. 
	    client.query(queryDropSPT, function(err,result){
	        if(err){
	            console.log("Error returning query: " + err);
	            client.end();
	            context.done("Fatal Error");
	        }
	       	console.log("Table Dropped.");
	       	createSPTSkeleton();
	    });
	    
	});
}

var createSPTSkeleton = function(){
	pg.connect(conString, function(err,client){
	    if(err){
	        return console.log("Connection error. ", err);
	    }
	
	    console.log("Connection Established.");
	
	    //Querying redshift. 
	    client.query(queryCreateSPTSkeleton, function(err,result){
	        if(err){
	            console.log("Error returning query: " + err);
	            client.end();
	            context.done("Fatal Error");
	        }
	       	console.log("Skeleton Table Created.");
	        makeSPTForTableau();
	    });
	});
}

var makeSPTForTableau = function(){
	pg.connect(conString, function(err,client){
	    if(err){
	        return console.log("Connection error. ", err);
	    }
	
	    console.log("Connection Established.");
	
	    //Querying redshift. 
	    client.query(queryMakeSPT, function(err,result){
	        if(err){
	            console.log("Error returning query: " + err);
	            client.end();
	            context.done("Fatal Error");
	        }
	       	console.log("SPT Table Ready.");
	       	createSPTTempToRemoveDuplicates();
	        
	    });
	    pg.end();
	});
}

var createSPTTempToRemoveDuplicates = function(){
	pg.connect(conString, function(err,client){
	    if(err){
	        return console.log("Connection error. ", err);
	    }
	
	    console.log("Connection Established.");
	
	    //Querying redshift. 
	    var querySPTTemp = "CREATE TABLE spt_temp(firstname varchar(200),lastname varchar(200),total_contract_price varchar(100),so_number varchar(100), so_promised_date date, so_status varchar(200), actual_del_date date,so_created_q varchar(100),so_created_ww varchar(100),so_created_date date, lead_create_q varchar(100),lead_create_ww varchar(100),lead_create_date date, so_delivery_date date, kw_ordered varchar(100),kw_system varchar(100),delivery_2_mp2 varchar(100),so_created_2_delivery varchar(100),pda_2_so_create varchar(100),final_completion_certificate varchar(200),inverter_quantity varchar(100),inverter_model_1 varchar(100),inverter_mfr_1 varchar(100),pv_modules_quantity varchar(50),pv_module_model varchar(50),pda_signed_date date,subsidiary_no_hierarchy varchar(200),street_1 varchar(5000),city varchar(100),zip varchar(50),tranche_id varchar(200),site_visit_date date,contract_canceled_date date,fully_executed_date date,assignment_agmnt_update_date date,assignment_agmnt_status varchar(100),commissioned_date date,pda_last_update date,pda_status varchar(200),site_visit_status varchar(100),contract_status_update date,contract_status varchar(100),cc_status_update date,credit_check_status varchar(100),proposal_status_update date,kw_proposed varchar(50),customer_id varchar(50),tsm varchar(200),site_id varchar(50),proposal_status varchar(100),sai_installer varchar(500),stage varchar(100),stage_sai_placeholder varchar(100),stage_sop_placeholder varchar(100),fully_executed_ww varchar(100),contract_2_site_visit varchar(100),contract_2_pda varchar(100),mp2_2_mp3 varchar(100),contract_2_mp2 varchar(100),contract_2_mp3 varchar(100),channel varchar(100),partner varchar(100),mp2_approval_date date,mp3_approval_date date,cc_status_ww varchar(100),cc_status_q varchar(100),contract_status_update_ww varchar(100),contract_status_update_q varchar(100),fully_executed_q varchar(100),site_visit_ww varchar(100),site_visit_q varchar(100),pda_complete_date date,pda_complete_ww varchar(100),pda_complete_q varchar(100),commissioned_ww varchar(100),commissioned_q varchar(100),mp2_apprvd_date date,mp2_apprvd_ww varchar(100),mp2_apprvd_q varchar(100),mp3_apprvd_date date,mp3_apprvd_ww varchar(100),mp3_apprvd_q varchar(100),assignment_agmnt_ww varchar(100),assignment_agmnt_q varchar(100),financing_program varchar(100), permit_status varchar(50), permit_date date, install_status varchar(200), target_install_date date, actual_install_date date, sales_agent varchar(200));";
	    client.query(querySPTTemp, function(err,result){
	        if(err){
	            console.log("Error returning query: " + err);
	            client.end();
	            context.done("Fatal Error");
	        }
	       	console.log("SPT Temp Created.");
	       	filterUniqueSPTRows();
	    });
	    pg.end();
	});
}

var filterUniqueSPTRows = function(){
	pg.connect(conString, function(err,client){
	    if(err){
	        return console.log("Connection error. ", err);
	    }
	
	    console.log("Connection Established.");
	
	    //Querying redshift. 
	    var queryFilterSPT = "INSERT INTO spt_temp SELECT DISTINCT * FROM spt";
	    client.query(queryFilterSPT, function(err,result){
	        if(err){
	            console.log("Error returning query: " + err);
	            client.end();
	            context.done("Fatal Error");
	        }
	       	console.log("SPT Filtered into SPT_TEMP.");
	       	dropSPTTable();
	    });
	    pg.end();
	});
}

var dropSPTTable = function(){
	pg.connect(conString, function(err,client){
	    if(err){
	        return console.log("Connection error. ", err);
	    }
	
	    console.log("Connection Established.");
	
	    //Querying redshift. 
	    var queryDropSPTForFilter = "drop table spt";
	    client.query(queryDropSPTForFilter, function(err,result){
	        if(err){
	            console.log("Error returning query: " + err);
	            client.end();
	            context.done("Fatal Error");
	        }
	       	console.log("SPT Dropped");
	       	renameSPTTempToSPT();
	    });
	    pg.end();
	});
}

var renameSPTTempToSPT = function(){
	pg.connect(conString, function(err,client){
	    if(err){
	        return console.log("Connection error. ", err);
	    }
	
	    console.log("Connection Established.");
	
	    //Querying redshift. 
	    var queryRenameSPTTempToSPT = "ALTER TABLE spt_temp rename to SPT";
	    client.query(queryRenameSPTTempToSPT, function(err,result){
	        if(err){
	            console.log("Error returning query: " + err);
	            client.end();
	            context.done("Fatal Error");
	        }
	       	console.log("SPT_Temp renamed.");
	    });
	    pg.end();
	});
}
