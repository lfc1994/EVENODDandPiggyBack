/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred.gridmix;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.DeprecationDelta;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.tools.rumen.JobStory;
import org.apache.hadoop.tools.rumen.JobStoryProducer;
import org.apache.hadoop.tools.rumen.Pre21JobHistoryConstants;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Emulation of Distributed Cache Usage in gridmix.
 * <br> Emulation of Distributed Cache Load in gridmix will put load on
 * TaskTrackers and affects execution time of tasks because of localization of
 * distributed cache files by TaskTrackers.
 * <br> Gridmix creates distributed cache files for simulated jobs by launching
 * a MapReduce job {@link GenerateDistCacheData} in advance i.e. before
 * launching simulated jobs.
 * <br> The distributed cache file paths used in the original cluster are mapped
 * to unique file names in the simulated cluster.
 * <br> All HDFS-based distributed cache files generated by gridmix are
 * public distributed cache files. But Gridmix makes sure that load incurred due
 * to localization of private distributed cache files on the original cluster
 * is also faithfully simulated. Gridmix emulates the load due to private
 * distributed cache files by mapping private distributed cache files of
 * different users in the original cluster to different public distributed cache
 * files in the simulated cluster.
 *
 * <br> The configuration properties like
 * {@link MRJobConfig#CACHE_FILES}, {@link MRJobConfig#CACHE_FILE_VISIBILITIES},
 * {@link MRJobConfig#CACHE_FILES_SIZES} and
 * {@link MRJobConfig#CACHE_FILE_TIMESTAMPS} obtained from trace are used to
 *  decide
 * <li> file size of each distributed cache file to be generated
 * <li> whether a distributed cache file is already seen in this trace file
 * <li> whether a distributed cache file was considered public or private.
 * <br>
 * <br> Gridmix configures these generated files as distributed cache files for
 * the simulated jobs.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class DistributedCacheEmulator {
  private static final Log LOG =
      LogFactory.getLog(DistributedCacheEmulator.class);

  static final long AVG_BYTES_PER_MAP = 128 * 1024 * 1024L;// 128MB

  private Path distCachePath;

  /**
   * Map between simulated cluster's distributed cache file paths and their
   * file sizes. Unique distributed cache files are entered into this map.
   * 2 distributed cache files are considered same if and only if their
   * file paths, visibilities and timestamps are same.
   */
  private Map<String, Long> distCacheFiles = new HashMap<String, Long>();

  /**
   * Configuration property for whether gridmix should emulate
   * distributed cache usage or not. Default value is true.
   */
  static final String GRIDMIX_EMULATE_DISTRIBUTEDCACHE =
      "gridmix.distributed-cache-emulation.enable";

  // Whether to emulate distributed cache usage or not
  boolean emulateDistributedCache = true;

  // Whether to generate distributed cache data or not
  boolean generateDistCacheData = false;

  Configuration conf; // gridmix configuration

  private static final Charset charsetUTF8 = Charset.forName("UTF-8");

  // Pseudo local file system where local FS based distributed cache files are
  // created by gridmix.
  FileSystem pseudoLocalFs = null;

  {
    // Need to handle deprecation of these MapReduce-internal configuration
    // properties as MapReduce doesn't handle their deprecation.
    Configuration.addDeprecations(new DeprecationDelta[] {
      new DeprecationDelta("mapred.cache.files.filesizes",
          MRJobConfig.CACHE_FILES_SIZES),
      new DeprecationDelta("mapred.cache.files.visibilities",
          MRJobConfig.CACHE_FILE_VISIBILITIES)
    });
  }

  /**
   * @param conf gridmix configuration
   * @param ioPath &lt;ioPath&gt;/distributedCache/ is the gridmix Distributed
   *               Cache directory
   */
  public DistributedCacheEmulator(Configuration conf, Path ioPath) {
    this.conf = conf;
    distCachePath = new Path(ioPath, "distributedCache");
    this.conf.setClass("fs.pseudo.impl", PseudoLocalFs.class, FileSystem.class);
  }

  /**
   * This is to be called before any other method of DistributedCacheEmulator.
   * <br> Checks if emulation of distributed cache load is needed and is feasible.
   *  Sets the flags generateDistCacheData and emulateDistributedCache to the
   *  appropriate values.
   * <br> Gridmix does not emulate distributed cache load if
   * <ol><li> the specific gridmix job type doesn't need emulation of
   * distributed cache load OR
   * <li> the trace is coming from a stream instead of file OR
   * <li> the distributed cache dir where distributed cache data is to be
   * generated by gridmix is on local file system OR
   * <li> execute permission is not there for any of the ascendant directories
   * of &lt;ioPath&gt; till root. This is because for emulation of distributed
   * cache load, distributed cache files created under
   * &lt;ioPath/distributedCache/&gt; should be considered by hadoop
   * as public distributed cache files.
   * <li> creation of pseudo local file system fails.</ol>
   * <br> For (2), (3), (4) and (5), generation of distributed cache data
   * is also disabled.
   * 
   * @param traceIn trace file path. If this is '-', then trace comes from the
   *                stream stdin.
   * @param jobCreator job creator of gridmix jobs of a specific type
   * @param generate  true if -generate option was specified
   * @throws IOException
   */
  void init(String traceIn, JobCreator jobCreator, boolean generate)
      throws IOException {
    emulateDistributedCache = jobCreator.canEmulateDistCacheLoad()
        && conf.getBoolean(GRIDMIX_EMULATE_DISTRIBUTEDCACHE, true);
    generateDistCacheData = generate;

    if (generateDistCacheData || emulateDistributedCache) {
      if ("-".equals(traceIn)) {// trace is from stdin
        LOG.warn("Gridmix will not emulate Distributed Cache load because "
            + "the input trace source is a stream instead of file.");
        emulateDistributedCache = generateDistCacheData = false;
      } else if (FileSystem.getLocal(conf).getUri().getScheme().equals(
          distCachePath.toUri().getScheme())) {// local FS
        LOG.warn("Gridmix will not emulate Distributed Cache load because "
            + "<iopath> provided is on local file system.");
        emulateDistributedCache = generateDistCacheData = false;
      } else {
        // Check if execute permission is there for all the ascendant
        // directories of distCachePath till root.
        FileSystem fs = FileSystem.get(conf);
        Path cur = distCachePath.getParent();
        while (cur != null) {
          if (cur.toString().length() > 0) {
            FsPermission perm = fs.getFileStatus(cur).getPermission();
            if (!perm.getOtherAction().and(FsAction.EXECUTE).equals(
                FsAction.EXECUTE)) {
              LOG.warn("Gridmix will not emulate Distributed Cache load "
                  + "because the ascendant directory (of distributed cache "
                  + "directory) " + cur + " doesn't have execute permission "
                  + "for others.");
              emulateDistributedCache = generateDistCacheData = false;
              break;
            }
          }
          cur = cur.getParent();
        }
      }
    }

    // Check if pseudo local file system can be created
    try {
      pseudoLocalFs = FileSystem.get(new URI("pseudo:///"), conf);
    } catch (URISyntaxException e) {
      LOG.warn("Gridmix will not emulate Distributed Cache load because "
          + "creation of pseudo local file system failed.");
      e.printStackTrace();
      emulateDistributedCache = generateDistCacheData = false;
      return;
    }
  }

  /**
   * @return true if gridmix should emulate distributed cache load
   */
  boolean shouldEmulateDistCacheLoad() {
    return emulateDistributedCache;
  }

  /**
   * @return true if gridmix should generate distributed cache data
   */
  boolean shouldGenerateDistCacheData() {
    return generateDistCacheData;
  }

  /**
   * @return the distributed cache directory path
   */
  Path getDistributedCacheDir() {
    return distCachePath;
  }

  /**
   * Create distributed cache directories.
   * Also create a file that contains the list of distributed cache files
   * that will be used as distributed cache files for all the simulated jobs.
   * @param jsp job story producer for the trace
   * @return exit code
   * @throws IOException
   */
  int setupGenerateDistCacheData(JobStoryProducer jsp)
      throws IOException {

    createDistCacheDirectory();
    return buildDistCacheFilesList(jsp);
  }

  /**
   * Create distributed cache directory where distributed cache files will be
   * created by the MapReduce job {@link GenerateDistCacheData#JOB_NAME}.
   * @throws IOException
   */
  private void createDistCacheDirectory() throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileSystem.mkdirs(fs, distCachePath, new FsPermission((short) 0777));
  }

  /**
   * Create the list of unique distributed cache files needed for all the
   * simulated jobs and write the list to a special file.
   * @param jsp job story producer for the trace
   * @return exit code
   * @throws IOException
   */
  private int buildDistCacheFilesList(JobStoryProducer jsp) throws IOException {
    // Read all the jobs from the trace file and build the list of unique
    // distributed cache files.
    JobStory jobStory;
    while ((jobStory = jsp.getNextJob()) != null) {
      if (jobStory.getOutcome() == Pre21JobHistoryConstants.Values.SUCCESS && 
         jobStory.getSubmissionTime() >= 0) {
        updateHDFSDistCacheFilesList(jobStory);
      }
    }
    jsp.close();

    return writeDistCacheFilesList();
  }

  /**
   * For the job to be simulated, identify the needed distributed cache files by
   * mapping original cluster's distributed cache file paths to the simulated cluster's
   * paths and add these paths in the map {@code distCacheFiles}.
   *<br>
   * JobStory should contain distributed cache related properties like
   * <li> {@link MRJobConfig#CACHE_FILES}
   * <li> {@link MRJobConfig#CACHE_FILE_VISIBILITIES}
   * <li> {@link MRJobConfig#CACHE_FILES_SIZES}
   * <li> {@link MRJobConfig#CACHE_FILE_TIMESTAMPS}
   * <li> {@link MRJobConfig#CLASSPATH_FILES}
   *
   * <li> {@link MRJobConfig#CACHE_ARCHIVES}
   * <li> {@link MRJobConfig#CACHE_ARCHIVES_VISIBILITIES}
   * <li> {@link MRJobConfig#CACHE_ARCHIVES_SIZES}
   * <li> {@link MRJobConfig#CACHE_ARCHIVES_TIMESTAMPS}
   * <li> {@link MRJobConfig#CLASSPATH_ARCHIVES}
   *
   * <li> {@link MRJobConfig#CACHE_SYMLINK}
   *
   * @param jobdesc JobStory of original job obtained from trace
   * @throws IOException
   */
  void updateHDFSDistCacheFilesList(JobStory jobdesc) throws IOException {

    // Map original job's distributed cache file paths to simulated cluster's
    // paths, to be used by this simulated job.
    JobConf jobConf = jobdesc.getJobConf();

    String[] files = jobConf.getStrings(MRJobConfig.CACHE_FILES);
    if (files != null) {

      String[] fileSizes = jobConf.getStrings(MRJobConfig.CACHE_FILES_SIZES);
      String[] visibilities =
        jobConf.getStrings(MRJobConfig.CACHE_FILE_VISIBILITIES);
      String[] timeStamps =
        jobConf.getStrings(MRJobConfig.CACHE_FILE_TIMESTAMPS);

      FileSystem fs = FileSystem.get(conf);
      String user = jobConf.getUser();
      for (int i = 0; i < files.length; i++) {
        // Check if visibilities are available because older hadoop versions
        // didn't have public, private Distributed Caches separately.
        boolean visibility =
            (visibilities == null) || Boolean.parseBoolean(visibilities[i]);
        if (isLocalDistCacheFile(files[i], user, visibility)) {
          // local FS based distributed cache file.
          // Create this file on the pseudo local FS on the fly (i.e. when the
          // simulated job is submitted).
          continue;
        }
        // distributed cache file on hdfs
        String mappedPath = mapDistCacheFilePath(files[i], timeStamps[i],
                                                 visibility, user);

        // No need to add a distributed cache file path to the list if
        // (1) the mapped path is already there in the list OR
        // (2) the file with the mapped path already exists.
        // In any of the above 2 cases, file paths, timestamps, file sizes and
        // visibilities match. File sizes should match if file paths and
        // timestamps match because single file path with single timestamp
        // should correspond to a single file size.
        if (distCacheFiles.containsKey(mappedPath) ||
            fs.exists(new Path(mappedPath))) {
          continue;
        }
        distCacheFiles.put(mappedPath, Long.valueOf(fileSizes[i]));
      }
    }
  }

  /**
   * Check if the file path provided was constructed by MapReduce for a
   * distributed cache file on local file system.
   * @param filePath path of the distributed cache file
   * @param user job submitter of the job for which &lt;filePath&gt; is a
   *             distributed cache file
   * @param visibility <code>true</code> for public distributed cache file
   * @return true if the path provided is of a local file system based
   *              distributed cache file
   */
  static boolean isLocalDistCacheFile(String filePath, String user,
                                       boolean visibility) {
    return (!visibility && filePath.contains(user + "/.staging"));
  }

  /**
   * Map the HDFS based distributed cache file path from original cluster to
   * a unique file name on the simulated cluster.
   * <br> Unique  distributed file names on simulated cluster are generated
   * using original cluster's <li>file path, <li>timestamp and <li> the
   * job-submitter for private distributed cache file.
   * <br> This implies that if on original cluster, a single HDFS file
   * considered as two private distributed cache files for two jobs of
   * different users, then the corresponding simulated jobs will have two
   * different files of the same size in public distributed cache, one for each
   * user. Both these simulated jobs will not share these distributed cache
   * files, thus leading to the same load as seen in the original cluster.
   * @param file distributed cache file path
   * @param timeStamp time stamp of dist cachce file
   * @param isPublic true if this distributed cache file is a public
   *                 distributed cache file
   * @param user job submitter on original cluster
   * @return the mapped path on simulated cluster
   */
  private String mapDistCacheFilePath(String file, String timeStamp,
      boolean isPublic, String user) {
    String id = file + timeStamp;
    if (!isPublic) {
      // consider job-submitter for private distributed cache file
      id = id.concat(user);
    }
    return new Path(distCachePath, MD5Hash.digest(id).toString()).toUri()
               .getPath();
  }

  /**
   * Write the list of distributed cache files in the decreasing order of
   * file sizes into the sequence file. This file will be input to the job
   * {@link GenerateDistCacheData}.
   * Also validates if -generate option is missing and distributed cache files
   * are missing.
   * @return exit code
   * @throws IOException
   */
  private int writeDistCacheFilesList()
      throws IOException {
    // Sort the distributed cache files in the decreasing order of file sizes.
    List dcFiles = new ArrayList(distCacheFiles.entrySet());
    Collections.sort(dcFiles, new Comparator() {
      public int compare(Object dc1, Object dc2) {
        return ((Comparable) ((Map.Entry) (dc2)).getValue())
            .compareTo(((Map.Entry) (dc1)).getValue());
      }
    });

    // write the sorted distributed cache files to the sequence file
    FileSystem fs = FileSystem.get(conf);
    Path distCacheFilesList = new Path(distCachePath, "_distCacheFiles.txt");
    conf.set(GenerateDistCacheData.GRIDMIX_DISTCACHE_FILE_LIST,
        distCacheFilesList.toString());
    SequenceFile.Writer src_writer = SequenceFile.createWriter(fs, conf,
        distCacheFilesList, LongWritable.class, BytesWritable.class,
        SequenceFile.CompressionType.NONE);

    // Total number of unique distributed cache files
    int fileCount = dcFiles.size();
    long byteCount = 0;// Total size of all distributed cache files
    long bytesSync = 0;// Bytes after previous sync;used to add sync marker

    for (Iterator it = dcFiles.iterator(); it.hasNext();) {
      Map.Entry entry = (Map.Entry)it.next();
      LongWritable fileSize =
          new LongWritable(Long.parseLong(entry.getValue().toString()));
      BytesWritable filePath =
          new BytesWritable(
          entry.getKey().toString().getBytes(charsetUTF8));

      byteCount += fileSize.get();
      bytesSync += fileSize.get();
      if (bytesSync > AVG_BYTES_PER_MAP) {
        src_writer.sync();
        bytesSync = fileSize.get();
      }
      src_writer.append(fileSize, filePath);
    }
    if (src_writer != null) {
      src_writer.close();
    }
    // Set delete on exit for 'dist cache files list' as it is not needed later.
    fs.deleteOnExit(distCacheFilesList);

    conf.setInt(GenerateDistCacheData.GRIDMIX_DISTCACHE_FILE_COUNT, fileCount);
    conf.setLong(GenerateDistCacheData.GRIDMIX_DISTCACHE_BYTE_COUNT, byteCount);
    LOG.info("Number of HDFS based distributed cache files to be generated is "
        + fileCount + ". Total size of HDFS based distributed cache files "
        + "to be generated is " + byteCount);

    if (!shouldGenerateDistCacheData() && fileCount > 0) {
      LOG.error("Missing " + fileCount + " distributed cache files under the "
          + " directory\n" + distCachePath + "\nthat are needed for gridmix"
          + " to emulate distributed cache load. Either use -generate\noption"
          + " to generate distributed cache data along with input data OR "
          + "disable\ndistributed cache emulation by configuring '"
          + DistributedCacheEmulator.GRIDMIX_EMULATE_DISTRIBUTEDCACHE
          + "' to false.");
      return Gridmix.MISSING_DIST_CACHE_FILES_ERROR;
    }
    return 0;
  }

  /**
   * If gridmix needs to emulate distributed cache load, then configure
   * distributed cache files of a simulated job by mapping the original
   * cluster's distributed cache file paths to the simulated cluster's paths and
   * setting these mapped paths in the job configuration of the simulated job.
   * <br>
   * Configure local FS based distributed cache files through the property
   * "tmpfiles" and hdfs based distributed cache files through the property
   * {@link MRJobConfig#CACHE_FILES}.
   * @param conf configuration for the simulated job to be run
   * @param jobConf job configuration of original cluster's job, obtained from
   *                trace
   * @throws IOException
   */
  void configureDistCacheFiles(Configuration conf, JobConf jobConf)
      throws IOException {
    if (shouldEmulateDistCacheLoad()) {

      String[] files = jobConf.getStrings(MRJobConfig.CACHE_FILES);
      if (files != null) {
        // hdfs based distributed cache files to be configured for simulated job
        List<String> cacheFiles = new ArrayList<String>();
        // local FS based distributed cache files to be configured for
        // simulated job
        List<String> localCacheFiles = new ArrayList<String>();

        String[] visibilities =
          jobConf.getStrings(MRJobConfig.CACHE_FILE_VISIBILITIES);
        String[] timeStamps =
          jobConf.getStrings(MRJobConfig.CACHE_FILE_TIMESTAMPS);
        String[] fileSizes = jobConf.getStrings(MRJobConfig.CACHE_FILES_SIZES);

        String user = jobConf.getUser();
        for (int i = 0; i < files.length; i++) {
          // Check if visibilities are available because older hadoop versions
          // didn't have public, private Distributed Caches separately.
          boolean visibility =
              (visibilities == null) || Boolean.parseBoolean(visibilities[i]);
          if (isLocalDistCacheFile(files[i], user, visibility)) {
            // local FS based distributed cache file.
            // Create this file on the pseudo local FS.
            String fileId = MD5Hash.digest(files[i] + timeStamps[i]).toString();
            long fileSize = Long.parseLong(fileSizes[i]);
            Path mappedLocalFilePath =
                PseudoLocalFs.generateFilePath(fileId, fileSize)
                    .makeQualified(pseudoLocalFs.getUri(),
                                   pseudoLocalFs.getWorkingDirectory());
            pseudoLocalFs.create(mappedLocalFilePath);
            localCacheFiles.add(mappedLocalFilePath.toUri().toString());
          } else {
            // hdfs based distributed cache file.
            // Get the mapped HDFS path on simulated cluster
            String mappedPath = mapDistCacheFilePath(files[i], timeStamps[i],
                                                     visibility, user);
            cacheFiles.add(mappedPath);
          }
        }
        if (cacheFiles.size() > 0) {
          // configure hdfs based distributed cache files for simulated job
          conf.setStrings(MRJobConfig.CACHE_FILES,
                          cacheFiles.toArray(new String[cacheFiles.size()]));
        }
        if (localCacheFiles.size() > 0) {
          // configure local FS based distributed cache files for simulated job
          conf.setStrings("tmpfiles", localCacheFiles.toArray(
                                        new String[localCacheFiles.size()]));
        }
      }
    }
  }
}
