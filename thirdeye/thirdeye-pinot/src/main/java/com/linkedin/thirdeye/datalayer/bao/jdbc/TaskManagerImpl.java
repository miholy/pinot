package com.linkedin.thirdeye.datalayer.bao.jdbc;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;

import com.google.inject.persist.Transactional;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.datalayer.pojo.AbstractBean;
import com.linkedin.thirdeye.datalayer.pojo.TaskBean;
import com.linkedin.thirdeye.datalayer.util.Predicate;

public class TaskManagerImpl extends AbstractManagerImpl<TaskDTO> implements TaskManager {

  private static final String FIND_BY_JOB_ID_STATUS_NOT_IN =
      "SELECT at FROM TaskDTO at " + "WHERE at.job.id = :jobId " + "AND at.status != :status";

  private static final String FIND_BY_STATUS_ORDER_BY_CREATE_TIME_ASC =
      " WHERE status = :status order by startTime asc";

  private static final String FIND_BY_STATUS_AND_LAST_MODIFIED_TIME_LT_EXPIRE =
      " WHERE status = :status AND lastModified < :expireTimestamp";

  public TaskManagerImpl() {
    super(TaskDTO.class, TaskBean.class);
  }
  
  public Long save(TaskDTO entity) {
    if (entity.getId() != null) {
      //TODO: throw exception and force the caller to call update instead
      update(entity);
      return entity.getId();
    }
    TaskBean bean = (TaskBean) convertDTO2Bean(entity, TaskBean.class);
    bean.setJobId(entity.getJob().getId());
    Long id = genericPojoDao.put(bean);
    entity.setId(id);
    return id;

  }

  @Override
  public List<TaskDTO> findByJobIdStatusNotIn(Long jobId, TaskStatus status) {
    //    return getEntityManager().createQuery(FIND_BY_JOB_ID_STATUS_NOT_IN, entityClass)
    //        .setParameter("jobId", jobId)
    //        .setParameter("status", status).getResultList();
    Predicate jobIdPredicate = Predicate.EQ("jobId", jobId);
    Predicate statusPredicate = Predicate.NEQ("status", status.toString());
    List<TaskBean> list =
        genericPojoDao.get(Predicate.AND(statusPredicate, jobIdPredicate), TaskBean.class);
    List<TaskDTO> result = new ArrayList<>();
    for (TaskBean bean : list) {
      result.add((TaskDTO) MODEL_MAPPER.map(bean, TaskDTO.class));
    }
    return result;
  }

  @Override
  public List<TaskDTO> findByStatusOrderByCreateTimeAsc(TaskStatus status, int fetchSize) {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("status", status.toString());
    List<TaskBean> list = genericPojoDao.executeParameterizedSQL(
        FIND_BY_STATUS_ORDER_BY_CREATE_TIME_ASC, parameterMap, TaskBean.class);
    List<TaskDTO> result = new ArrayList<>();
    for (TaskBean bean : list) {
      result.add((TaskDTO) MODEL_MAPPER.map(bean, TaskDTO.class));
    }
    return result;
  }

  @Override
  public boolean updateStatusAndWorkerId(Long workerId, Long id, TaskStatus oldStatus,
      TaskStatus newStatus) {
    TaskDTO task = findById(id);
    if (task.getStatus().equals(oldStatus)) {
      task.setStatus(newStatus);
      task.setWorkerId(workerId);
      save(task);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void updateStatusAndTaskEndTime(Long id, TaskStatus oldStatus, TaskStatus newStatus,
      Long taskEndTime) {
    TaskDTO task = findById(id);
    if (task.getStatus().equals(oldStatus)) {
      task.setStatus(newStatus);
      task.setEndTime(taskEndTime);
      save(task);
    }
  }

  @Override
  @Transactional
  public int deleteRecordsOlderThanDaysWithStatus(int days, TaskStatus status) {
    DateTime expireDate = new DateTime().minusDays(days);
    Timestamp expireTimestamp = new Timestamp(expireDate.getMillis());

    Predicate timestampPredicate = Predicate.LT("createTime", expireTimestamp);
    Predicate statusPredicate = Predicate.EQ("status", status.toString());
    List<TaskBean> list =
        genericPojoDao.get(Predicate.AND(statusPredicate, timestampPredicate), TaskBean.class);
    for (TaskBean bean : list) {
      deleteById(bean.getId());
    }
    return list.size();
  }
}
