CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.CurrencyConversion` AS 
SELECT
  CurrencyConversion.MANDT AS Client_MANDT,
  CurrencyConversion.KURST AS ExchangeRateType_KURST,
  CurrencyConversion.FCURR AS FromCurrency_FCURR,
  CurrencyConversion.TCURR AS ToCurrency_TCURR,
  CurrencyConversion.UKURS AS ExchangeRate_UKURS,
  CurrencyConversion.start_date AS StartDate,
  CurrencyConversion.end_date AS EndDate,
  CurrencyConversion.conv_date AS ConvDate
FROM
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt }}.currency_conversion` AS CurrencyConversion