package org.apache.kylin.source.hive;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;

import java.util.Map;

public class KylinCubeConfig {
  private static final Logger logger = LoggerFactory.getLogger(KylinCubeConfig.class);

  public static String generateHiveSetStatements(IJoinedFlatTableDesc flatTableDesc) {
    KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
    CubeManager cubeMgr = CubeManager.getInstance(kylinConfig);
    KylinConfig kylinCubeConfig = flatTableDesc.getDataModel().getConfig();
    Map<String,String> mapReduceOverrides = kylinCubeConfig.getMRConfigOverride();
    Map<String,String> tezOverrides = kylinCubeConfig.getTEZConfigOverride();
    String setStatement = "\n";
    for (String k : mapReduceOverrides.keySet()) {
      setStatement += "SET " + k + "=" + mapReduceOverrides.get(k) + ";\n";
    }

    for (String k : tezOverrides.keySet()) {
      setStatement += "SET " + k + "=" + tezOverrides.get(k) + ";\n";
    }

    logger.info("Kylin Hive set statements overrides: ");
    logger.info(setStatement);
    return setStatement;
  }
}
