package org.sunbird.learner.util;

import java.util.Map;
import org.sunbird.common.ElasticSearchHelper;
import org.sunbird.common.factory.EsClientFactory;
import org.sunbird.common.inf.ElasticSearchService;
import org.sunbird.common.models.util.JsonKey;
import org.sunbird.common.models.util.LoggerEnum;
import org.sunbird.common.models.util.ProjectLogger;
import org.sunbird.common.models.util.ProjectUtil;
import scala.concurrent.Future;

public class CourseBatchUtil {
  private static ElasticSearchService esUtil = EsClientFactory.getInstance(JsonKey.REST);

  private CourseBatchUtil() {}

  public static void syncCourseBatchForeground(String uniqueId, Map<String, Object> req) {
    ProjectLogger.log(
        "CourseBatchManagementActor: syncCourseBatchForeground called for course batch ID = "
            + uniqueId,
        LoggerEnum.INFO.name());
    req.put(JsonKey.ID, uniqueId);
    req.put(JsonKey.IDENTIFIER, uniqueId);
    Future<String> esResponseF =
        esUtil.save(ProjectUtil.EsType.courseBatch.getTypeName(), uniqueId, req);
    String esResponse = (String) ElasticSearchHelper.getResponseFromFuture(esResponseF);

    ProjectLogger.log(
        "CourseBatchManagementActor::syncCourseBatchForeground: Sync response for course batch ID = "
            + uniqueId
            + " received response = "
            + esResponse,
        LoggerEnum.INFO.name());
  }
}
