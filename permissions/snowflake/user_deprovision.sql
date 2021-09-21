-- using the SECURITYADMIN role run the following by setting 'USER_NAME' provided in Offboarding or Deprovisioning issue request.

ALTER USER IF EXISTS USER_NAME SET DISABLED = TRUE;