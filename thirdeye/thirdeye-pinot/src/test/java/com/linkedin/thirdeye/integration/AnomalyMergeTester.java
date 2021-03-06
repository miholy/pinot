package com.linkedin.thirdeye.integration;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeConfig;
import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeExecutor;
import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeStrategy;
import com.linkedin.thirdeye.common.persistence.PersistenceUtil;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.hibernate.AnomalyFunctionManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.hibernate.MergedAnomalyResultManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.hibernate.RawAnomalyResultManagerImpl;

public class AnomalyMergeTester {

  private static final String dbConfig = "/opt/Code/thirdeye-configs/sandbox-configs/persistence.yml";
  public static void main(String[] args) throws IOException, InterruptedException {
    PersistenceUtil.init(new File(dbConfig));
    AnomalyMergeConfig config = new AnomalyMergeConfig();
    config.setMergeStrategy(AnomalyMergeStrategy.FUNCTION_DIMENSIONS);
    config.setMergeDuration(-1);
    config.setSequentialAllowedGap(2 * 60 * 60_000); // 2 hours

    RawAnomalyResultManagerImpl resultDAO = PersistenceUtil.getInstance(RawAnomalyResultManagerImpl.class);
    MergedAnomalyResultManagerImpl mergedResultDAO = PersistenceUtil.getInstance(MergedAnomalyResultManagerImpl.class);
    AnomalyFunctionManager functionDAO = PersistenceUtil.getInstance(AnomalyFunctionManagerImpl.class);
    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    AnomalyMergeExecutor anomalyMergeExecutor = new AnomalyMergeExecutor(mergedResultDAO, functionDAO, resultDAO, executorService);
    System.out.println("Starting in 1 minute");
    executorService.scheduleWithFixedDelay(anomalyMergeExecutor, 1, 2, TimeUnit.MINUTES);
    int ch = '0';
    while(ch != 'q') {
      System.out.println("press q to quit");
      ch = System.in.read();
      if(ch == 'q') {
        executorService.shutdown();
        break;
      }
    }
  }
}
