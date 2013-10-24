/*
 * $Id$
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package com.novemberain.quartz.mongodb;

import static com.novemberain.quartz.mongodb.Keys.JOB_KEY_GROUP;
import static com.novemberain.quartz.mongodb.Keys.JOB_KEY_NAME;
import static com.novemberain.quartz.mongodb.Keys.KEY_GROUP;
import static com.novemberain.quartz.mongodb.Keys.KEY_NAME;
import static com.novemberain.quartz.mongodb.Keys.LOCK_KEY_GROUP;
import static com.novemberain.quartz.mongodb.Keys.LOCK_KEY_NAME;
import static com.novemberain.quartz.mongodb.Keys.keyToDBObject;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.MongoException.DuplicateKey;
import com.mongodb.QueryBuilder;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.types.ObjectId;
import org.quartz.Calendar;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBJobStore implements JobStore, Constants {
  protected final Logger log = LoggerFactory.getLogger(this.getClass());

  public static final DBObject KEY_AND_GROUP_FIELDS = BasicDBObjectBuilder
      .start().append(KEY_GROUP, 1).append(KEY_NAME, 1).get();

  private Mongo mongo;
  private String collectionPrefix = "quartz_";
  private String dbName;
  private DBCollection jobCollection;
  private DBCollection triggerCollection;
  private DBCollection calendarCollection;
  private ClassLoadHelper loadHelper;
  private DBCollection locksCollection;
  private DBCollection pausedTriggerGroupsCollection;
  private DBCollection pausedJobGroupsCollection;
  private String instanceId;
  private String[] addresses;
  private String username;
  private String password;
  private SchedulerSignaler signaler;
  protected long misfireThreshold = 5000l;
  private long triggerTimeoutMillis = 10 * 60 * 1000L;

  private List<TriggerPersistenceHelper> persistenceHelpers;
  private QueryHelper queryHelper;

  @Override
  public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler)
      throws SchedulerConfigException {
    this.loadHelper = loadHelper;
    this.signaler = signaler;

    if (this.addresses == null || this.addresses.length == 0) {
      throw new SchedulerConfigException(
          "At least one MongoDB address must be specified.");
    }

    this.mongo = this.connectToMongoDB();

    DB db = this.selectDatabase(this.mongo);
    this.initializeCollections(db);
    this.ensureIndexes();

    this.initializeHelpers();
  }

  @Override
  public void schedulerStarted() throws SchedulerException {
    // No-op
  }

  @Override
  public void schedulerPaused() {
    // No-op
  }

  @Override
  public void schedulerResumed() {
  }

  @Override
  public void shutdown() {
    this.mongo.close();
  }

  @Override
  public boolean supportsPersistence() {
    return true;
  }

  @Override
  public long getEstimatedTimeToReleaseAndAcquireTrigger() {
    // this will vary...
    return 200;
  }

  @Override
  public boolean isClustered() {
    return true;
  }

  /**
   * Job and Trigger storage Methods
   */
  @Override
  public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    ObjectId jobId = this.storeJobInMongo(newJob, false);

    this.log.debug("Storing job " + newJob.getKey() + " and trigger "
        + newTrigger.getKey());
    this.storeTrigger(newTrigger, jobId, false);
  }

  @Override
  public void storeJob(JobDetail newJob, boolean replaceExisting)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    this.storeJobInMongo(newJob, replaceExisting);
  }

  @Override
  public void storeJobsAndTriggers(
      Map<JobDetail, List<Trigger>> triggersAndJobs, boolean replace)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
    BasicDBObject keyObject = Keys.keyToDBObject(jobKey);
    DBCursor find = this.jobCollection.find(keyObject);
    try {
      if (find.hasNext()) {
        DBObject jobObj = find.next();
        this.jobCollection.remove(keyObject);
        this.triggerCollection.remove(new BasicDBObject(TRIGGER_JOB_ID, jobObj
            .get("_id")));

        return true;
      }

      return false;
    } finally {
      find.close();
    }
  }

  @Override
  public boolean removeJobs(List<JobKey> jobKeys)
      throws JobPersistenceException {
    for (JobKey key : jobKeys) {
      this.removeJob(key);
    }
    return false;
  }

  @Override
  @SuppressWarnings("unchecked")
  public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
    DBObject dbObject = this.findJobDocumentByKey(jobKey);
    if (dbObject == null) {
      // Return null if job does not exist, per interface
      return null;
    }

    try {
      Class<Job> jobClass = (Class<Job>) this.getJobClassLoader().loadClass(
          (String) dbObject.get(JOB_CLASS));

      JobBuilder builder = JobBuilder
          .newJob(jobClass)
          .withIdentity((String) dbObject.get(JOB_KEY_NAME),
              (String) dbObject.get(JOB_KEY_GROUP))
          .withDescription((String) dbObject.get(JOB_DESCRIPTION));

      JobDataMap jobData = new JobDataMap();
      for (String key : dbObject.keySet()) {
        if (!key.equals(JOB_KEY_NAME) && !key.equals(JOB_KEY_GROUP)
            && !key.equals(JOB_CLASS) && !key.equals(JOB_DESCRIPTION)
            && !key.equals("_id")) {
          jobData.put(key, dbObject.get(key));
        }
      }

      return builder.usingJobData(jobData).build();
    } catch (ClassNotFoundException e) {
      throw new JobPersistenceException("Could not load job class "
          + dbObject.get(JOB_CLASS), e);
    }
  }

  @Override
  public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    if (newTrigger.getJobKey() == null) {
      throw new JobPersistenceException(
          "Trigger must be associated with a job. Please specify a JobKey.");
    }

    DBObject dbObject = this.jobCollection.findOne(Keys
        .keyToDBObject(newTrigger.getJobKey()));
    if (dbObject != null) {
      this.storeTrigger(newTrigger, (ObjectId) dbObject.get("_id"),
          replaceExisting);
    } else {
      throw new JobPersistenceException("Could not find job with key "
          + newTrigger.getJobKey());
    }
  }

  // If the removal of the Trigger results in an 'orphaned' Job that is not
  // 'durable',
  // then the job should be removed also.
  @Override
  public boolean removeTrigger(TriggerKey triggerKey)
      throws JobPersistenceException {
    BasicDBObject dbObject = Keys.keyToDBObject(triggerKey);
    DBCursor triggers = this.triggerCollection.find(dbObject);
    try {
      if (triggers.count() > 0) {
        DBObject trigger = triggers.next();
        if (trigger.containsField(TRIGGER_JOB_ID)) {
          // There is only 1 job per trigger so no need to look further than 1
          // job.
          DBObject job = this.jobCollection.findOne(new BasicDBObject("_id",
              trigger.get(TRIGGER_JOB_ID)));
          // Durability is not yet implemented in MongoDBJOBStore so next will
          // always be true
          if (!job.containsField(JOB_DURABILITY)
              || job.get(JOB_DURABILITY).toString() == "false") {
            // Check if this job is referenced by other triggers.
            BasicDBObject query = new BasicDBObject();
            query.put(TRIGGER_JOB_ID, job.get("_id"));
            DBCursor referencedTriggers = this.triggerCollection.find(query);
            try {
              if (referencedTriggers.count() <= 1) {
                this.jobCollection.remove(job);
              }
            } finally {
              referencedTriggers.close();
            }
          }
        } else {
          this.log.debug("The triggger had no associated jobs");
        }
        this.triggerCollection.remove(dbObject);

        return true;
      }

      return false;
    } finally {
      triggers.close();
    }
  }

  @Override
  public boolean removeTriggers(List<TriggerKey> triggerKeys)
      throws JobPersistenceException {
    for (TriggerKey key : triggerKeys) {
      this.removeTrigger(key);
    }
    return false;
  }

  @Override
  public boolean replaceTrigger(TriggerKey triggerKey,
      OperableTrigger newTrigger) throws JobPersistenceException {
    this.removeTrigger(triggerKey);
    this.storeTrigger(newTrigger, false);
    return true;
  }

  @Override
  public OperableTrigger retrieveTrigger(TriggerKey triggerKey)
      throws JobPersistenceException {
    DBObject dbObject = this.triggerCollection.findOne(Keys
        .keyToDBObject(triggerKey));
    if (dbObject == null) {
      return null;
    }
    return this.toTrigger(triggerKey, dbObject);
  }

  @Override
  public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
    return this.jobCollection.count(Keys.keyToDBObject(jobKey)) > 0;
  }

  @Override
  public boolean checkExists(TriggerKey triggerKey)
      throws JobPersistenceException {
    return this.triggerCollection.count(Keys.keyToDBObject(triggerKey)) > 0;
  }

  @Override
  public void clearAllSchedulingData() throws JobPersistenceException {
    this.jobCollection.remove(new BasicDBObject());
    this.triggerCollection.remove(new BasicDBObject());
    this.calendarCollection.remove(new BasicDBObject());
    this.pausedJobGroupsCollection.remove(new BasicDBObject());
    this.pausedTriggerGroupsCollection.remove(new BasicDBObject());
  }

  @Override
  public void storeCalendar(String name, Calendar calendar,
      boolean replaceExisting, boolean updateTriggers)
      throws ObjectAlreadyExistsException, JobPersistenceException {
    // TODO
    if (updateTriggers) {
      throw new UnsupportedOperationException(
          "Updating triggers is not supported.");
    }

    BasicDBObject dbObject = new BasicDBObject();
    dbObject.put(CALENDAR_NAME, name);
    dbObject.put(CALENDAR_SERIALIZED_OBJECT, this.serialize(calendar));

    this.calendarCollection.insert(dbObject);
  }

  @Override
  public boolean removeCalendar(String calName) throws JobPersistenceException {
    BasicDBObject searchObj = new BasicDBObject(CALENDAR_NAME, calName);
    if (this.calendarCollection.count(searchObj) > 0) {
      this.calendarCollection.remove(searchObj);
      return true;
    }
    return false;
  }

  @Override
  public Calendar retrieveCalendar(String calName)
      throws JobPersistenceException {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNumberOfJobs() throws JobPersistenceException {
    return (int) this.jobCollection.count();
  }

  @Override
  public int getNumberOfTriggers() throws JobPersistenceException {
    return (int) this.triggerCollection.count();
  }

  @Override
  public int getNumberOfCalendars() throws JobPersistenceException {
    return (int) this.calendarCollection.count();
  }

  public int getNumberOfLocks() {
    return (int) this.locksCollection.count();
  }

  @Override
  public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher)
      throws JobPersistenceException {
    Set<JobKey> result = new HashSet<JobKey>();

    DBCursor cursor = this.jobCollection.find(
        this.queryHelper.matchingKeysConditionFor(matcher),
        KEY_AND_GROUP_FIELDS);
    try {
      while (cursor.hasNext()) {
        DBObject dbo = cursor.next();
        JobKey key = Keys.dbObjectToJobKey(dbo);
        result.add(key);
      }
    } finally {
      cursor.close();
    }

    return result;
  }

  @Override
  public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher)
      throws JobPersistenceException {
    Set<TriggerKey> result = new HashSet<TriggerKey>();

    DBCursor cursor = this.triggerCollection.find(
        this.queryHelper.matchingKeysConditionFor(matcher),
        KEY_AND_GROUP_FIELDS);

    try {
      while (cursor.hasNext()) {
        DBObject dbo = cursor.next();
        TriggerKey key = Keys.dbObjectToTriggerKey(dbo);
        result.add(key);
      }
    } finally {
      cursor.close();
    }

    return result;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<String> getJobGroupNames() throws JobPersistenceException {
    return new ArrayList<String>(this.jobCollection.distinct(KEY_GROUP));
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<String> getTriggerGroupNames() throws JobPersistenceException {
    return new ArrayList<String>(this.triggerCollection.distinct(KEY_GROUP));
  }

  @Override
  public List<String> getCalendarNames() throws JobPersistenceException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<OperableTrigger> getTriggersForJob(JobKey jobKey)
      throws JobPersistenceException {
    DBObject dbObject = this.findJobDocumentByKey(jobKey);

    List<OperableTrigger> triggers = new ArrayList<OperableTrigger>();

    DBCursor cursor = this.triggerCollection.find(new BasicDBObject(
        TRIGGER_JOB_ID, dbObject.get("_id")));
    try {
      while (cursor.hasNext()) {
        triggers.add(this.toTrigger(cursor.next()));
      }
    } finally {
      cursor.close();
    }

    return triggers;
  }

  @Override
  public TriggerState getTriggerState(TriggerKey triggerKey)
      throws JobPersistenceException {
    DBObject doc = this.findTriggerDocumentByKey(triggerKey);

    return this.triggerStateForValue((String) doc.get(TRIGGER_STATE));
  }

  @Override
  public void pauseTrigger(TriggerKey triggerKey)
      throws JobPersistenceException {
    this.triggerCollection.update(Keys.keyToDBObject(triggerKey),
        this.updateThatSetsTriggerStateTo(STATE_PAUSED));
  }

  @Override
  public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher)
      throws JobPersistenceException {
    final GroupHelper groupHelper = new GroupHelper(this.triggerCollection,
        this.queryHelper);
    this.triggerCollection.update(
        this.queryHelper.matchingKeysConditionFor(matcher),
        this.updateThatSetsTriggerStateTo(STATE_PAUSED), false, true);

    final Set<String> set = groupHelper.groupsThatMatch(matcher);
    this.markTriggerGroupsAsPaused(set);

    return set;
  }

  @Override
  public void resumeTrigger(TriggerKey triggerKey)
      throws JobPersistenceException {
    // TODO: port blocking behavior and misfired triggers handling from
    // StdJDBCDelegate in Quartz
    this.triggerCollection.update(Keys.keyToDBObject(triggerKey),
        this.updateThatSetsTriggerStateTo(STATE_WAITING));
  }

  @Override
  public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher)
      throws JobPersistenceException {
    final GroupHelper groupHelper = new GroupHelper(this.triggerCollection,
        this.queryHelper);
    this.triggerCollection.update(
        this.queryHelper.matchingKeysConditionFor(matcher),
        this.updateThatSetsTriggerStateTo(STATE_WAITING), false, true);

    final Set<String> set = groupHelper.groupsThatMatch(matcher);
    this.unmarkTriggerGroupsAsPaused(set);
    return set;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
    return new HashSet<String>(
        this.pausedTriggerGroupsCollection.distinct(KEY_GROUP));
  }

  @SuppressWarnings("unchecked")
  public Set<String> getPausedJobGroups() throws JobPersistenceException {
    return new HashSet<String>(
        this.pausedJobGroupsCollection.distinct(KEY_GROUP));
  }

  @Override
  public void pauseAll() throws JobPersistenceException {
    final GroupHelper groupHelper = new GroupHelper(this.triggerCollection,
        this.queryHelper);
    this.triggerCollection.update(new BasicDBObject(),
        this.updateThatSetsTriggerStateTo(STATE_PAUSED));
    this.markTriggerGroupsAsPaused(groupHelper.allGroups());
  }

  @Override
  public void resumeAll() throws JobPersistenceException {
    final GroupHelper groupHelper = new GroupHelper(this.triggerCollection,
        this.queryHelper);
    this.triggerCollection.update(new BasicDBObject(),
        this.updateThatSetsTriggerStateTo(STATE_WAITING));
    this.unmarkTriggerGroupsAsPaused(groupHelper.allGroups());
  }

  @Override
  public void pauseJob(JobKey jobKey) throws JobPersistenceException {
    final ObjectId jobId = (ObjectId) this.findJobDocumentByKey(jobKey).get(
        "_id");
    final TriggerGroupHelper groupHelper = new TriggerGroupHelper(
        this.triggerCollection, this.queryHelper);
    List<String> groups = groupHelper.groupsForJobId(jobId);
    this.triggerCollection.update(new BasicDBObject(TRIGGER_JOB_ID, jobId),
        this.updateThatSetsTriggerStateTo(STATE_PAUSED));
    this.markTriggerGroupsAsPaused(groups);
  }

  @Override
  public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher)
      throws JobPersistenceException {
    final TriggerGroupHelper groupHelper = new TriggerGroupHelper(
        this.triggerCollection, this.queryHelper);
    List<String> groups = groupHelper.groupsForJobIds(this.idsFrom(this
        .findJobDocumentsThatMatch(groupMatcher)));
    this.triggerCollection.update(this.queryHelper.inGroups(groups),
        this.updateThatSetsTriggerStateTo(STATE_PAUSED));
    this.markJobGroupsAsPaused(groups);

    return groups;
  }

  @Override
  public void resumeJob(JobKey jobKey) throws JobPersistenceException {
    final ObjectId jobId = (ObjectId) this.findJobDocumentByKey(jobKey).get(
        "_id");
    // TODO: port blocking behavior and misfired triggers handling from
    // StdJDBCDelegate in Quartz
    this.triggerCollection.update(new BasicDBObject(TRIGGER_JOB_ID, jobId),
        this.updateThatSetsTriggerStateTo(STATE_WAITING));
  }

  @Override
  public Collection<String> resumeJobs(GroupMatcher<JobKey> groupMatcher)
      throws JobPersistenceException {
    final TriggerGroupHelper groupHelper = new TriggerGroupHelper(
        this.triggerCollection, this.queryHelper);
    List<String> groups = groupHelper.groupsForJobIds(this.idsFrom(this
        .findJobDocumentsThatMatch(groupMatcher)));
    this.triggerCollection.update(this.queryHelper.inGroups(groups),
        this.updateThatSetsTriggerStateTo(STATE_WAITING));
    this.unmarkJobGroupsAsPaused(groups);

    return groups;
  }

  @Override
  public List<OperableTrigger> acquireNextTriggers(long noLaterThan,
      int maxCount, long timeWindow) throws JobPersistenceException {
    BasicDBObject query = new BasicDBObject();
    query.put(TRIGGER_NEXT_FIRE_TIME, new BasicDBObject("$lte", new Date(
        noLaterThan)));

    if (this.log.isDebugEnabled()) {
      this.log.debug("Finding up to " + maxCount
          + " triggers which have time less than " + new Date(noLaterThan));
    }

    List<OperableTrigger> triggers = new ArrayList<OperableTrigger>();
    DBCursor cursor = this.triggerCollection.find(query);
    try {
      BasicDBObject sort = new BasicDBObject();
      sort.put(TRIGGER_NEXT_FIRE_TIME, Integer.valueOf(1));
      cursor.sort(sort);

      if (this.log.isDebugEnabled()) {
        this.log.debug("Found " + cursor.count()
            + " triggers which are eligible to be run.");
      }

      while (cursor.hasNext() && maxCount > triggers.size()) {
        DBObject dbObj = cursor.next();

        BasicDBObject lock = new BasicDBObject();
        lock.put(LOCK_KEY_NAME, dbObj.get(KEY_NAME));
        lock.put(LOCK_KEY_GROUP, dbObj.get(KEY_GROUP));
        lock.put(LOCK_INSTANCE_ID, this.instanceId);
        lock.put(LOCK_TIME, new Date());

        try {
          OperableTrigger trigger = this.toTrigger(dbObj);

          if (trigger.getNextFireTime() == null) {
            if (this.log.isDebugEnabled()) {
              this.log.debug("Skipping trigger " + trigger.getKey()
                  + " as it has no next fire time.");
            }

            continue;
          }

          // deal with misfires
          if (this.applyMisfire(trigger) && trigger.getNextFireTime() == null) {
            if (this.log.isDebugEnabled()) {
              this.log
                  .debug("Skipping trigger "
                      + trigger.getKey()
                      + " as it has no next fire time after the misfire was applied.");
            }

            continue;
          }
          this.log.debug("Inserting lock for trigger " + trigger.getKey());
          this.locksCollection.insert(lock);
          this.log.debug("Aquired trigger " + trigger.getKey());
          triggers.add(trigger);
        } catch (DuplicateKey e) {

          OperableTrigger trigger = this.toTrigger(dbObj);

          // someone else acquired this lock. Move on.
          this.log.debug("Failed to acquire trigger " + trigger.getKey()
              + " due to a lock");

          lock = new BasicDBObject();
          lock.put(LOCK_KEY_NAME, dbObj.get(KEY_NAME));
          lock.put(LOCK_KEY_GROUP, dbObj.get(KEY_GROUP));

          DBObject existingLock;
          DBCursor lockCursor = this.locksCollection.find(lock);
          try {
            if (lockCursor.hasNext()) {
              existingLock = lockCursor.next();
            } else {
              this.log
                  .warn("Error retrieving expired lock from the database. Maybe it was deleted");
              return this
                  .acquireNextTriggers(noLaterThan, maxCount, timeWindow);
            }

            // support for trigger lock expirations
            if (this.isTriggerLockExpired(existingLock)) {
              this.log
                  .warn("Lock for trigger "
                      + trigger.getKey()
                      + " is expired - removing lock and retrying trigger acquisition");
              this.removeTriggerLock(trigger);
              return this
                  .acquireNextTriggers(noLaterThan, maxCount, timeWindow);
            }
          } finally {
            lockCursor.close();
          }
        }
      }
    } finally {
      cursor.close();
    }

    return triggers;
  }

  @Override
  public void releaseAcquiredTrigger(OperableTrigger trigger)
      throws JobPersistenceException {
    try {
      this.removeTriggerLock(trigger);
    } catch (Exception e) {
      throw new JobPersistenceException(e.getLocalizedMessage(), e);
    }
  }

  @Override
  public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers)
      throws JobPersistenceException {

    List<TriggerFiredResult> results = new ArrayList<TriggerFiredResult>();

    for (OperableTrigger trigger : triggers) {
      this.log.debug("Fired trigger " + trigger.getKey());
      Calendar cal = null;
      if (trigger.getCalendarName() != null) {
        cal = this.retrieveCalendar(trigger.getCalendarName());
        if (cal == null) {
          continue;
        }
      }

      Date prevFireTime = trigger.getPreviousFireTime();

      TriggerFiredBundle bndle = new TriggerFiredBundle(
          this.retrieveJob(trigger), trigger, cal, false, new Date(),
          trigger.getPreviousFireTime(), prevFireTime,
          trigger.getNextFireTime());

      JobDetail job = bndle.getJobDetail();

      if (job.isConcurrentExectionDisallowed()) {
        throw new UnsupportedOperationException(
            "ConcurrentExecutionDisallowed is not supported currently.");
      }
      results.add(new TriggerFiredResult(bndle));

      trigger.triggered(cal);
      this.storeTrigger(trigger, true);

    }
    return results;
  }

  @Override
  public void triggeredJobComplete(OperableTrigger trigger,
      JobDetail jobDetail, CompletedExecutionInstruction triggerInstCode)
      throws JobPersistenceException {
    this.log.debug("Trigger completed " + trigger.getKey());
    // check for trigger deleted during execution...
    OperableTrigger trigger2 = this.retrieveTrigger(trigger.getKey());
    if (trigger2 != null) {
      if (triggerInstCode == CompletedExecutionInstruction.DELETE_TRIGGER) {
        if (trigger.getNextFireTime() == null) {
          // double check for possible reschedule within job
          // execution, which would cancel the need to delete...
          if (trigger2.getNextFireTime() == null) {
            this.removeTrigger(trigger.getKey());
          }
        } else {
          this.removeTrigger(trigger.getKey());
          this.signaler.signalSchedulingChange(0L);
        }
      } else if (triggerInstCode == CompletedExecutionInstruction.SET_TRIGGER_COMPLETE) {
        // TODO: need to store state
        this.signaler.signalSchedulingChange(0L);
      } else if (triggerInstCode == CompletedExecutionInstruction.SET_TRIGGER_ERROR) {
        // TODO: need to store state
        this.signaler.signalSchedulingChange(0L);
      } else if (triggerInstCode == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR) {
        // TODO: need to store state
        this.signaler.signalSchedulingChange(0L);
      } else if (triggerInstCode == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_COMPLETE) {
        // TODO: need to store state
        this.signaler.signalSchedulingChange(0L);
      }
    }

    this.removeTriggerLock(trigger);
  }

  @Override
  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  @Override
  public void setInstanceName(String schedName) {
    // No-op
  }

  @Override
  public void setThreadPoolSize(int poolSize) {
    // No-op
  }

  public void setAddresses(String addresses) {
    this.addresses = addresses.split(",");
  }

  public DBCollection getJobCollection() {
    return this.jobCollection;
  }

  public DBCollection getTriggerCollection() {
    return this.triggerCollection;
  }

  public DBCollection getCalendarCollection() {
    return this.calendarCollection;
  }

  public DBCollection getLocksCollection() {
    return this.locksCollection;
  }

  public String getDbName() {
    return this.dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void setCollectionPrefix(String prefix) {
    this.collectionPrefix = prefix + "_";
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public long getMisfireThreshold() {
    return this.misfireThreshold;
  }

  public void setMisfireThreshold(long misfireThreshold) {
    this.misfireThreshold = misfireThreshold;
  }

  public void setTriggerTimeoutMillis(long triggerTimeoutMillis) {
    this.triggerTimeoutMillis = triggerTimeoutMillis;
  }

  //
  // Implementation
  //

  private void initializeCollections(DB db) {
    this.jobCollection = db.getCollection(this.collectionPrefix + "jobs");
    this.triggerCollection = db.getCollection(this.collectionPrefix
        + "triggers");
    this.calendarCollection = db.getCollection(this.collectionPrefix
        + "calendars");
    this.locksCollection = db.getCollection(this.collectionPrefix + "locks");

    this.pausedTriggerGroupsCollection = db.getCollection(this.collectionPrefix
        + "paused_trigger_groups");
    this.pausedJobGroupsCollection = db.getCollection(this.collectionPrefix
        + "paused_job_groups");
  }

  private DB selectDatabase(Mongo mongo) {
    DB db = this.mongo.getDB(this.dbName);
    // MongoDB defaults are insane, set a reasonable write concern explicitly.
    // MK.
    db.setWriteConcern(WriteConcern.JOURNAL_SAFE);
    if (this.username != null) {
      db.authenticate(this.username, this.password.toCharArray());
    }
    return db;
  }

  private Mongo connectToMongoDB() throws SchedulerConfigException {
    MongoClientOptions options = MongoClientOptions.builder()
        .writeConcern(WriteConcern.ACKNOWLEDGED).build();

    try {
      ArrayList<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();
      for (String a : this.addresses) {
        serverAddresses.add(new ServerAddress(a));
      }
      return new MongoClient(serverAddresses, options);

    } catch (UnknownHostException e) {
      throw new SchedulerConfigException("Could not connect to MongoDB.", e);
    } catch (MongoException e) {
      throw new SchedulerConfigException("Could not connect to MongoDB.", e);
    }
  }

  protected OperableTrigger toTrigger(DBObject dbObj)
      throws JobPersistenceException {
    TriggerKey key = new TriggerKey((String) dbObj.get(KEY_NAME),
        (String) dbObj.get(KEY_GROUP));
    return this.toTrigger(key, dbObj);
  }

  protected OperableTrigger toTrigger(TriggerKey triggerKey, DBObject dbObject)
      throws JobPersistenceException {
    OperableTrigger trigger;
    try {
      @SuppressWarnings("unchecked")
      Class<OperableTrigger> triggerClass = (Class<OperableTrigger>) this
          .getTriggerClassLoader().loadClass(
              (String) dbObject.get(TRIGGER_CLASS));
      trigger = triggerClass.newInstance();
    } catch (ClassNotFoundException e) {
      throw new JobPersistenceException("Could not find trigger class "
          + (String) dbObject.get(TRIGGER_CLASS));
    } catch (Exception e) {
      throw new JobPersistenceException("Could not instantiate trigger class "
          + (String) dbObject.get(TRIGGER_CLASS));
    }

    TriggerPersistenceHelper tpd = this.triggerPersistenceDelegateFor(trigger);

    trigger.setKey(triggerKey);
    trigger.setCalendarName((String) dbObject.get(TRIGGER_CALENDAR_NAME));
    trigger.setDescription((String) dbObject.get(TRIGGER_DESCRIPTION));
    trigger.setStartTime((Date) dbObject.get(TRIGGER_START_TIME));
    trigger.setEndTime((Date) dbObject.get(TRIGGER_END_TIME));
    trigger.setFireInstanceId((String) dbObject.get(TRIGGER_FIRE_INSTANCE_ID));
    trigger.setMisfireInstruction((Integer) dbObject
        .get(TRIGGER_MISFIRE_INSTRUCTION));
    trigger.setNextFireTime((Date) dbObject.get(TRIGGER_NEXT_FIRE_TIME));
    trigger
        .setPreviousFireTime((Date) dbObject.get(TRIGGER_PREVIOUS_FIRE_TIME));
    trigger.setPriority((Integer) dbObject.get(TRIGGER_PRIORITY));

    trigger = tpd.setExtraPropertiesAfterInstantiation(trigger, dbObject);

    DBObject job = this.jobCollection.findOne(new BasicDBObject("_id", dbObject
        .get(TRIGGER_JOB_ID)));
    if (job != null) {
      trigger.setJobKey(new JobKey((String) job.get(JOB_KEY_NAME), (String) job
          .get(JOB_KEY_GROUP)));
      return trigger;
    } else {
      // job was deleted
      return null;
    }
  }

  protected ClassLoader getTriggerClassLoader() {
    return org.quartz.Job.class.getClassLoader();
  }

  private TriggerPersistenceHelper triggerPersistenceDelegateFor(
      OperableTrigger trigger) {
    TriggerPersistenceHelper result = null;

    for (TriggerPersistenceHelper d : this.persistenceHelpers) {
      if (d.canHandleTriggerType(trigger)) {
        result = d;
        break;
      }
    }

    assert result != null;
    return result;
  }

  protected boolean isTriggerLockExpired(DBObject lock) {
    Date lockTime = (Date) lock.get(LOCK_TIME);
    long elaspedTime = System.currentTimeMillis() - lockTime.getTime();
    return (elaspedTime > this.triggerTimeoutMillis);
  }

  protected boolean applyMisfire(OperableTrigger trigger)
      throws JobPersistenceException {
    long misfireTime = System.currentTimeMillis();
    if (this.getMisfireThreshold() > 0) {
      misfireTime -= this.getMisfireThreshold();
    }

    Date tnft = trigger.getNextFireTime();
    if (tnft == null
        || tnft.getTime() > misfireTime
        || trigger.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) {
      return false;
    }

    Calendar cal = null;
    if (trigger.getCalendarName() != null) {
      cal = this.retrieveCalendar(trigger.getCalendarName());
    }

    this.signaler.notifyTriggerListenersMisfired((OperableTrigger) trigger
        .clone());

    trigger.updateAfterMisfire(cal);

    if (trigger.getNextFireTime() == null) {
      this.signaler.notifySchedulerListenersFinalized(trigger);
    } else if (tnft.equals(trigger.getNextFireTime())) {
      return false;
    }

    this.storeTrigger(trigger, true);
    return true;
  }

  private Object serialize(Calendar calendar) throws JobPersistenceException {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    try {
      ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
      objectStream.writeObject(calendar);
      objectStream.close();
      return byteStream.toByteArray();
    } catch (IOException e) {
      throw new JobPersistenceException("Could not serialize Calendar.", e);
    }
  }

  private void ensureIndexes() {
    BasicDBObject keys = new BasicDBObject();
    keys.put(JOB_KEY_NAME, 1);
    keys.put(JOB_KEY_GROUP, 1);
    this.jobCollection.ensureIndex(keys, null, true);

    keys = new BasicDBObject();
    keys.put(KEY_NAME, 1);
    keys.put(KEY_GROUP, 1);
    this.triggerCollection.ensureIndex(keys, null, true);

    keys = new BasicDBObject();
    keys.put(LOCK_KEY_NAME, 1);
    keys.put(LOCK_KEY_GROUP, 1);
    this.locksCollection.ensureIndex(keys, null, true);
    // remove all locks for this instance on startup
    this.locksCollection.remove(new BasicDBObject(LOCK_INSTANCE_ID,
        this.instanceId));

    keys = new BasicDBObject();
    keys.put(CALENDAR_NAME, 1);
    this.calendarCollection.ensureIndex(keys, null, true);
  }

  protected void storeTrigger(OperableTrigger newTrigger, ObjectId jobId,
      boolean replaceExisting) throws ObjectAlreadyExistsException {
    BasicDBObject trigger = new BasicDBObject();
    trigger.put(TRIGGER_STATE, STATE_WAITING);
    trigger.put(TRIGGER_CALENDAR_NAME, newTrigger.getCalendarName());
    trigger.put(TRIGGER_CLASS, newTrigger.getClass().getName());
    trigger.put(TRIGGER_DESCRIPTION, newTrigger.getDescription());
    trigger.put(TRIGGER_END_TIME, newTrigger.getEndTime());
    trigger.put(TRIGGER_FINAL_FIRE_TIME, newTrigger.getFinalFireTime());
    trigger.put(TRIGGER_FIRE_INSTANCE_ID, newTrigger.getFireInstanceId());
    trigger.put(TRIGGER_JOB_ID, jobId);
    trigger.put(KEY_NAME, newTrigger.getKey().getName());
    trigger.put(KEY_GROUP, newTrigger.getKey().getGroup());
    trigger
        .put(TRIGGER_MISFIRE_INSTRUCTION, newTrigger.getMisfireInstruction());
    trigger.put(TRIGGER_NEXT_FIRE_TIME, newTrigger.getNextFireTime());
    trigger.put(TRIGGER_PREVIOUS_FIRE_TIME, newTrigger.getPreviousFireTime());
    trigger.put(TRIGGER_PRIORITY, newTrigger.getPriority());
    trigger.put(TRIGGER_START_TIME, newTrigger.getStartTime());

    TriggerPersistenceHelper tpd = this
        .triggerPersistenceDelegateFor(newTrigger);
    trigger = (BasicDBObject) tpd.injectExtraPropertiesForInsert(newTrigger,
        trigger);

    try {
      this.triggerCollection.insert(trigger);
    } catch (DuplicateKey key) {
      if (replaceExisting) {
        trigger.remove("_id");
        this.triggerCollection.update(keyToDBObject(newTrigger.getKey()),
            trigger);
      } else {
        throw new ObjectAlreadyExistsException(newTrigger);
      }
    }
  }

  protected ObjectId storeJobInMongo(JobDetail newJob, boolean replaceExisting)
      throws ObjectAlreadyExistsException {
    JobKey key = newJob.getKey();

    BasicDBObject job = keyToDBObject(key);

    if (replaceExisting) {
      DBObject result = this.jobCollection.findOne(job);
      if (result != null) {
        result = job;
      }
    }

    job.put(JOB_KEY_NAME, key.getName());
    job.put(JOB_KEY_GROUP, key.getGroup());
    job.put(JOB_DESCRIPTION, newJob.getDescription());
    job.put(JOB_CLASS, newJob.getJobClass().getName());
    job.put(JOB_DURABILITY, newJob.isDurable());

    job.putAll(newJob.getJobDataMap());

    try {
      this.jobCollection.insert(job);

      return (ObjectId) job.get("_id");
    } catch (DuplicateKey e) {
      throw new ObjectAlreadyExistsException(e.getMessage());
    }
  }

  protected void removeTriggerLock(OperableTrigger trigger) {
    this.log.debug("Removing trigger lock " + trigger.getKey() + "."
        + this.instanceId);
    BasicDBObject lock = new BasicDBObject();
    lock.put(LOCK_KEY_NAME, trigger.getKey().getName());
    lock.put(LOCK_KEY_GROUP, trigger.getKey().getGroup());

    // Coment this out, as expired trigger locks should be deleted by any
    // another instance
    // lock.put(LOCK_INSTANCE_ID, instanceId);

    this.locksCollection.remove(lock);
    this.log.debug("Trigger lock " + trigger.getKey() + "." + this.instanceId
        + " removed.");
  }

  protected ClassLoader getJobClassLoader() {
    return this.loadHelper.getClassLoader();
  }

  private JobDetail retrieveJob(OperableTrigger trigger)
      throws JobPersistenceException {
    try {
      return this.retrieveJob(trigger.getJobKey());
    } catch (JobPersistenceException e) {
      this.removeTriggerLock(trigger);
      throw e;
    }
  }

  protected DBObject findJobDocumentByKey(JobKey key) {
    return this.jobCollection.findOne(keyToDBObject(key));
  }

  protected DBObject findTriggerDocumentByKey(TriggerKey key) {
    return this.triggerCollection.findOne(keyToDBObject(key));
  }

  private void initializeHelpers() {
    this.persistenceHelpers = new ArrayList<TriggerPersistenceHelper>();

    this.persistenceHelpers.add(new SimpleTriggerPersistenceHelper());
    this.persistenceHelpers.add(new CalendarIntervalTriggerPersistenceHelper());
    this.persistenceHelpers.add(new CronTriggerPersistenceHelper());
    this.persistenceHelpers
        .add(new DailyTimeIntervalTriggerPersistenceHelper());

    this.queryHelper = new QueryHelper();
  }

  private TriggerState triggerStateForValue(String ts) {
    if (ts == null) {
      return TriggerState.NONE;
    }

    if (ts.equals(STATE_DELETED)) {
      return TriggerState.NONE;
    }

    if (ts.equals(STATE_COMPLETE)) {
      return TriggerState.COMPLETE;
    }

    if (ts.equals(STATE_PAUSED)) {
      return TriggerState.PAUSED;
    }

    if (ts.equals(STATE_PAUSED_BLOCKED)) {
      return TriggerState.PAUSED;
    }

    if (ts.equals(STATE_ERROR)) {
      return TriggerState.ERROR;
    }

    if (ts.equals(STATE_BLOCKED)) {
      return TriggerState.BLOCKED;
    }

    // waiting or acquired
    return TriggerState.NORMAL;
  }

  private DBObject updateThatSetsTriggerStateTo(String state) {
    return BasicDBObjectBuilder.start("$set",
        new BasicDBObject(TRIGGER_STATE, state)).get();
  }

  private void markTriggerGroupsAsPaused(Collection<String> groups) {
    List<DBObject> list = new ArrayList<DBObject>();
    for (String s : groups) {
      list.add(new BasicDBObject(KEY_GROUP, s));
    }
    this.pausedTriggerGroupsCollection.insert(list);
  }

  private void unmarkTriggerGroupsAsPaused(Collection<String> groups) {
    this.pausedTriggerGroupsCollection.remove(QueryBuilder.start(KEY_GROUP)
        .in(groups).get());
  }

  private void markJobGroupsAsPaused(List<String> groups) {
    if (groups == null) {
      throw new IllegalArgumentException("groups cannot be null!");
    }
    List<DBObject> list = new ArrayList<DBObject>();
    for (String s : groups) {
      list.add(new BasicDBObject(KEY_GROUP, s));
    }
    this.pausedJobGroupsCollection.insert(list);
  }

  private void unmarkJobGroupsAsPaused(Collection<String> groups) {
    this.pausedJobGroupsCollection.remove(QueryBuilder.start(KEY_GROUP)
        .in(groups).get());
  }

  private Collection<ObjectId> idsFrom(Collection<DBObject> docs) {
    // so much repetitive code would be gone if Java collections just had .map
    // and .filter…
    List<ObjectId> list = new ArrayList<ObjectId>();
    for (DBObject doc : docs) {
      list.add((ObjectId) doc.get("_id"));
    }
    return list;
  }

  private Collection<DBObject> findJobDocumentsThatMatch(
      GroupMatcher<JobKey> matcher) {
    final GroupHelper groupHelper = new GroupHelper(this.jobCollection,
        this.queryHelper);
    return groupHelper.inGroupsThatMatch(matcher);
  }
}
