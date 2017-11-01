using Sitecore.DataExchange.Attributes;
using Sitecore.DataExchange.Providers.Salesforce.Plugins;
using System;
using Sitecore.DataExchange.Contexts;
using System.Collections.Generic;
using Sitecore.Connect.Crm.Salesforce.Query;
using Sitecore.DataExchange.Providers.Salesforce.Extensions;

namespace Sitecore.Support.DataExchange.Providers.Salesforce.Processors.PipelineSteps
{
  [RequiredEndpointPlugins(typeof(ObjectDataStoreSettings))]
  public class ReadObjectsStepProcessor : Sitecore.DataExchange.Providers.Salesforce.Processors.PipelineSteps.ReadObjectsStepProcessor
  {
    protected override FilterExpression GetFilterExpression(ReadObjectsSettings stepSettings,
                                                                PipelineContext pipelineContext)
    {
      if (stepSettings == null)
      {
        return null;
      }
      if (pipelineContext == null || stepSettings.ExcludeUseDeltaSettings || !pipelineContext.HasDateRangeSettings())
      {
        return stepSettings.FilterExpression;
      }

      var dateRangeSettings = pipelineContext.GetDateRangeSettings();
      var expr = stepSettings.FilterExpression;

      List<String> lstConditions = null;

      if (null != expr)
      {
        lstConditions = expr.Conditions;
      }

      if (lstConditions == null)
        lstConditions = new List<string>();
      if (dateRangeSettings.LowerBound > Sitecore.Connect.Crm.Salesforce.Utility.Utility.MinSupportedDate())
      {
        if (expr == null)
        {
          expr = new FilterExpression();
          expr.FilterOperator = "And";
        }
        lstConditions.Add("LastModifiedDate >= " + dateRangeSettings.LowerBound.ToString("yyyy-MM-ddThh:mm:ssZ"));
        pipelineContext.PipelineBatchContext.Logger.Info("Condition added to read objects modified on or after {0}. (pipeline step: {1}, object: {2})", dateRangeSettings.LowerBound, pipelineContext.CurrentPipelineStep.Name, stepSettings.EntityName);
      }
      if (dateRangeSettings.UpperBound < System.Convert.ToDateTime("01/01/4000") &&
          dateRangeSettings.UpperBound > dateRangeSettings.LowerBound)
      {
        if (expr == null)
        {
          expr = new FilterExpression();
          expr.FilterOperator = "And";
        }
        lstConditions.Add("LastModifiedDate <= " + dateRangeSettings.UpperBound.ToString("yyyy-MM-ddThh:mm:ssZ"));

        pipelineContext.PipelineBatchContext.Logger.Info("Condition added to read objects modified on or before {0}. (pipeline step: {1}, object: {2})", dateRangeSettings.UpperBound, pipelineContext.CurrentPipelineStep.Name, stepSettings.EntityName);
      }

      if (null != expr)
      {
        expr.Conditions = lstConditions;
      }

      return expr;
    }
  }
}