/* =========================================================
   SILVER LAYER COMPLETE SCRIPT
   Project: pratap-dev-2026-01-483206
   ========================================================= */


/* =========================================================
   1. DEPARTMENTS
   ========================================================= */

CREATE TABLE IF NOT EXISTS `pratap-dev-2026-01-483206.silver_dataset.departments` (
    dept_id STRING,
    name STRING,
    is_quarantined BOOLEAN
);

TRUNCATE TABLE `pratap-dev-2026-01-483206.silver_dataset.departments`;

INSERT INTO `pratap-dev-2026-01-483206.silver_dataset.departments`
SELECT DISTINCT 
    deptid AS dept_id,
    name,
    CASE 
        WHEN deptid IS NULL OR name IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_quarantined
FROM `pratap-dev-2026-01-483206.bronze_dataset.departments`;


/* =========================================================
   2. PROVIDERS
   ========================================================= */

CREATE TABLE IF NOT EXISTS `pratap-dev-2026-01-483206.silver_dataset.providers` (
    ProviderID STRING,
    FirstName STRING,
    LastName STRING,
    Specialization STRING,
    DeptID STRING,
    NPI INT64,
    is_quarantined BOOLEAN
);

TRUNCATE TABLE `pratap-dev-2026-01-483206.silver_dataset.providers`;

INSERT INTO `pratap-dev-2026-01-483206.silver_dataset.providers`
SELECT DISTINCT 
    ProviderID,
    FirstName,
    LastName,
    Specialization,
    DeptID,
    CAST(NPI AS INT64) AS NPI,
    CASE 
        WHEN ProviderID IS NULL OR DeptID IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_quarantined
FROM `pratap-dev-2026-01-483206.bronze_dataset.providers`;


/* =========================================================
   3. PATIENTS (SCD TYPE 2)
   ========================================================= */

CREATE TABLE IF NOT EXISTS `pratap-dev-2026-01-483206.silver_dataset.patients` (
  PatientID STRING NOT NULL,
  FirstName STRING,
  LastName STRING,
  MiddleName STRING,
  SSN STRING,
  PhoneNumber STRING,
  Gender STRING,
  DOB DATE,
  Address STRING,
  ModifiedDate TIMESTAMP,
  is_quarantined BOOLEAN,
  start_date TIMESTAMP,
  end_date TIMESTAMP,
  is_current BOOLEAN,
  HashID_ChangeCheck STRING,
  SilverLoadTime TIMESTAMP
);

MERGE INTO `pratap-dev-2026-01-483206.silver_dataset.patients` AS target
USING (
  SELECT
    PatientID,
    FirstName,
    LastName,
    MiddleName,
    SSN,
    PhoneNumber,
    Gender,
    DATE(TIMESTAMP_MILLIS(CAST(DOB AS INT64))) AS DOB,
    Address,
    CURRENT_TIMESTAMP() AS CurrentLoadDate,
    IF(PatientID IS NULL OR SSN IS NULL, TRUE, FALSE) AS is_quarantined,
    TO_HEX(SHA256(CONCAT(
      IFNULL(FirstName, ''), IFNULL(LastName, ''), IFNULL(MiddleName, ''),
      IFNULL(SSN, ''), IFNULL(PhoneNumber, ''), IFNULL(Gender, ''),
      IFNULL(Address, '')
    ))) AS HashID_ChangeCheck
  FROM `pratap-dev-2026-01-483206.bronze_dataset.patients`
) AS source
ON target.PatientID = source.PatientID AND target.is_current = TRUE

WHEN MATCHED AND target.HashID_ChangeCheck != source.HashID_ChangeCheck
THEN UPDATE SET
  target.end_date = TIMESTAMP_SUB(source.CurrentLoadDate, INTERVAL 1 MICROSECOND),
  target.is_current = FALSE,
  target.SilverLoadTime = source.CurrentLoadDate

WHEN NOT MATCHED THEN
INSERT (
  PatientID, FirstName, LastName, MiddleName, SSN, PhoneNumber,
  Gender, DOB, Address, ModifiedDate, is_quarantined,
  start_date, end_date, is_current, HashID_ChangeCheck, SilverLoadTime
)
VALUES (
  source.PatientID, source.FirstName, source.LastName, source.MiddleName, source.SSN, source.PhoneNumber,
  source.Gender, source.DOB, source.Address, source.CurrentLoadDate, source.is_quarantined,
  source.CurrentLoadDate,
  TIMESTAMP('9999-12-31 23:59:59'),
  TRUE,
  source.HashID_ChangeCheck,
  source.CurrentLoadDate
)

WHEN NOT MATCHED BY SOURCE AND target.is_current = TRUE
THEN UPDATE SET
  target.end_date = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MICROSECOND),
  target.is_current = FALSE,
  target.SilverLoadTime = CURRENT_TIMESTAMP();


/* =========================================================
   4. ENCOUNTERS
   ========================================================= */

CREATE TABLE IF NOT EXISTS `pratap-dev-2026-01-483206.silver_dataset.encounters` (
  EncounterID STRING,
  PatientID STRING,
  EncounterDate DATE,
  EncounterType STRING,
  ProviderID STRING,
  DepartmentID STRING,
  ProcedureCode INT64,
  InsertedDate DATE,
  ModifiedDate DATE,
  is_quarantined BOOLEAN
);

TRUNCATE TABLE `pratap-dev-2026-01-483206.silver_dataset.encounters`;

INSERT INTO `pratap-dev-2026-01-483206.silver_dataset.encounters`
SELECT DISTINCT 
  EncounterID,
  PatientID,
  DATE(TIMESTAMP_MILLIS(CAST(EncounterDate AS INT64))),
  EncounterType,
  ProviderID,
  DepartmentID,
  CAST(ProcedureCode AS INT64),
  DATE(TIMESTAMP_MILLIS(CAST(InsertedDate AS INT64))),
  DATE(TIMESTAMP_MILLIS(CAST(ModifiedDate AS INT64))),
  CASE 
    WHEN EncounterID IS NULL OR PatientID IS NULL OR ProviderID IS NULL THEN TRUE 
    ELSE FALSE 
  END
FROM `pratap-dev-2026-01-483206.bronze_dataset.encounters`;


/* =========================================================
   5. TRANSACTIONS
   ========================================================= */

CREATE TABLE IF NOT EXISTS `pratap-dev-2026-01-483206.silver_dataset.transactions` (
  TransactionID STRING,
  EncounterID STRING,
  PatientID STRING,
  ProviderID STRING,
  DeptID STRING,
  VisitDate DATE,
  ServiceDate DATE,
  PaidDate DATE,
  VisitType STRING,
  Amount FLOAT64,
  AmountType STRING,
  PaidAmount FLOAT64,
  ClaimID STRING,
  PayorID STRING,
  ProcedureCode INT64,
  ICDCode STRING,
  LineOfBusiness STRING,
  MedicaidID STRING,
  MedicareID STRING,
  InsertDate DATE,
  ModifiedDate DATE,
  is_quarantined BOOLEAN
);

TRUNCATE TABLE `pratap-dev-2026-01-483206.silver_dataset.transactions`;

INSERT INTO `pratap-dev-2026-01-483206.silver_dataset.transactions`
SELECT DISTINCT 
  TransactionID,
  EncounterID,
  PatientID,
  ProviderID,
  DeptID,
  DATE(TIMESTAMP_MILLIS(CAST(VisitDate AS INT64))),
  DATE(TIMESTAMP_MILLIS(CAST(ServiceDate AS INT64))),
  DATE(TIMESTAMP_MILLIS(CAST(PaidDate AS INT64))),
  VisitType,
  CAST(Amount AS FLOAT64),
  AmountType,
  CAST(PaidAmount AS FLOAT64),
  ClaimID,
  PayorID,
  CAST(ProcedureCode AS INT64),
  ICDCode,
  LineOfBusiness,
  MedicaidID,
  MedicareID,
  DATE(TIMESTAMP_MILLIS(CAST(InsertDate AS INT64))),
  DATE(TIMESTAMP_MILLIS(CAST(ModifiedDate AS INT64))),
  CASE 
    WHEN TransactionID IS NULL OR PatientID IS NULL OR EncounterID IS NULL THEN TRUE 
    ELSE FALSE 
  END
FROM `pratap-dev-2026-01-483206.bronze_dataset.transactions`;


/* =========================================================
   6. CLAIMS
   ========================================================= */

CREATE TABLE IF NOT EXISTS `pratap-dev-2026-01-483206.silver_dataset.claims` (
    claim_id STRING,
    transaction_id STRING,
    patient_id STRING,
    encounter_id STRING,
    provider_id STRING,
    dept_id STRING,
    service_date DATE,
    claim_date DATE,
    payor_id STRING,
    claim_amount NUMERIC,
    paid_amount NUMERIC,
    claim_status STRING,
    payor_type STRING,
    deductible NUMERIC,
    coinsurance NUMERIC,
    copay NUMERIC,
    insert_date TIMESTAMP,
    modified_date TIMESTAMP,
    is_quarantined BOOLEAN
);

TRUNCATE TABLE `pratap-dev-2026-01-483206.silver_dataset.claims`;

INSERT INTO `pratap-dev-2026-01-483206.silver_dataset.claims`
SELECT
    t1.ClaimID,
    t1.TransactionID,
    t1.PatientID,
    t1.EncounterID,
    t1.ProviderID,
    t1.DeptID,
    SAFE_CAST(t1.ServiceDate AS DATE),
    SAFE_CAST(t1.ClaimDate AS DATE),
    t1.PayorID,
    SAFE_CAST(REPLACE(t1.ClaimAmount, '$', '') AS NUMERIC),
    SAFE_CAST(REPLACE(t1.PaidAmount, '$', '') AS NUMERIC),
    t1.ClaimStatus,
    t1.PayorType,
    SAFE_CAST(t1.Deductible AS NUMERIC),
    SAFE_CAST(t1.Coinsurance AS NUMERIC),
    SAFE_CAST(t1.Copay AS NUMERIC),
    SAFE_CAST(t1.InsertDate AS TIMESTAMP),
    SAFE_CAST(t1.ModifiedDate AS TIMESTAMP),
    CASE 
        WHEN t1.ClaimID IS NULL 
        OR t1.TransactionID IS NULL 
        OR t1.PatientID IS NULL
        OR t1.ClaimAmount IS NULL
        OR t1.PaidAmount IS NULL
        THEN TRUE 
        ELSE FALSE 
    END
FROM `pratap-dev-2026-01-483206.bronze_dataset.claims` t1;