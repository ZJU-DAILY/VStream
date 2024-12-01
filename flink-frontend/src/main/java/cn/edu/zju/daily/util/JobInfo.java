package cn.edu.zju.daily.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * This one assumes that the job is running in STREAMING mode, meaning all tasks are scheduled in
 * advance.
 */
@Slf4j
public class JobInfo implements Serializable {

    private static final Tuple2<String, String> DEFAULT_TUPLE = new Tuple2<>(null, null);

    private final String jobName;
    private final String jobId;

    // "taskName.subtaskId" -> ("host", "taskManagerId")
    private final Map<String, Tuple2<String, String>> subtaskToTM = new HashMap<>();
    // "taskName" -> parallelism
    private final Map<String, Integer> taskToParallelism = new HashMap<>();
    private final Map<String, String> taskToId = new HashMap<>();
    private final Map<String, String> taskIdToName = new HashMap<>();

    public JobInfo(String jobManagerHost, int jobManagerPort) {
        // query Flink REST API for task manager information
        Tuple2<String, String> nameIdPair =
                getFirstRunningJobNameAndId(jobManagerHost, jobManagerPort);
        while (nameIdPair == null) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
            }
            nameIdPair = getFirstRunningJobNameAndId(jobManagerHost, jobManagerPort);
        }
        jobName = nameIdPair.f0;
        jobId = nameIdPair.f1;
        getMapping(jobManagerHost, jobManagerPort, nameIdPair.f1);
        LOG.info("Got job info for {}", jobName);
    }

    public String getTaskId(String taskName) {
        return taskToId.get(taskName);
    }

    public String getTaskName(String taskId) {
        return taskIdToName.get(taskId);
    }

    public String getHost(String taskName, int subtaskIndex) {
        return subtaskToTM.get(taskName + "." + subtaskIndex).f0;
    }

    public Set<String> getTaskNames() {
        return taskToParallelism.keySet();
    }

    public String getTaskManagerId(String taskName, int subtaskIndex) {
        return subtaskToTM.get(taskName + "." + subtaskIndex).f1;
    }

    public int getTaskParallelism(String taskName) {
        return taskToParallelism.get(taskName);
    }

    public String getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    private void getMapping(String jobManagerHost, int jobManagerPort, String jobId) {
        String unassigned = "(unassigned)";
        tryGetMapping(jobManagerHost, jobManagerPort, jobId);
        while (subtaskToTM.isEmpty()
                || subtaskToTM.values().stream()
                        .anyMatch(t -> unassigned.equals(t.f0) || unassigned.equals(t.f1))) {
            // If not all assigned, wait and retry
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignored) {
            }
            subtaskToTM.clear();
            taskToParallelism.clear();
            taskToId.clear();
            taskIdToName.clear();
            tryGetMapping(jobManagerHost, jobManagerPort, jobId);
        }
    }

    private void tryGetMapping(String jobManagerHost, int jobManagerPort, String jobId) {
        // query Flink REST API for mappings
        JSONObject job =
                new JSONObject(httpGetJson(jobManagerHost, jobManagerPort, "/jobs/" + jobId));
        JSONArray vertices = job.getJSONArray("vertices");
        for (int i = 0; i < vertices.length(); i++) {
            JSONObject vertice = vertices.getJSONObject(i);
            String taskName = vertice.getString("name");
            String taskId = vertice.getString("id");
            taskToId.put(taskName, taskId);
            taskIdToName.put(taskId, taskName);
            JSONObject task =
                    new JSONObject(
                            httpGetJson(
                                    jobManagerHost,
                                    jobManagerPort,
                                    "/jobs/" + jobId + "/vertices/" + taskId));
            int parallelism = task.getInt("parallelism");
            taskToParallelism.put(taskName, parallelism);
            JSONArray subtasks = task.getJSONArray("subtasks");
            for (int j = 0; j < subtasks.length(); j++) {
                JSONObject subtask = subtasks.getJSONObject(j);
                String host = subtask.getString("host");
                String taskManagerId = subtask.getString("taskmanager-id");
                int subtaskIndex = subtask.getInt("subtask");
                subtaskToTM.put(taskName + "." + subtaskIndex, new Tuple2<>(host, taskManagerId));
            }
        }
    }

    private static Tuple2<String, String> getFirstRunningJobNameAndId(
            String jobManagerHost, int jobManagerPort) {
        try {
            JSONObject response =
                    new JSONObject(httpGetJson(jobManagerHost, jobManagerPort, "/jobs/overview"));
            JSONArray jobs = response.getJSONArray("jobs");
            for (int i = 0; i < jobs.length(); i++) {
                JSONObject job = jobs.getJSONObject(i);
                if (job.getString("state").equals("RUNNING")) {
                    return new Tuple2<>(job.getString("name"), job.getString("jid"));
                }
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private static String httpGetJson(String host, int port, String url) {
        HttpURLConnection connection = null;
        try {
            URL apiUrl = new URL("http://" + host + ":" + port + url);
            connection = (HttpURLConnection) apiUrl.openConnection();

            // Set up the connection properties
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setConnectTimeout(5000); // 5 seconds timeout
            connection.setReadTimeout(5000); // 5 seconds timeout

            // Get the response code
            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                return "";
            }

            // Read the response
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(connection.getInputStream()));
            StringBuilder response = new StringBuilder();
            String line;

            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            return response.toString();
        } catch (Exception ignored) {
            return "";
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
}
