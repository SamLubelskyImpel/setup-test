# Stress Testing

This test suite uses JMeter to stress test the CRM and DMS APIs.

# How to use JMeter

To install and get started with JMeter, you can follow their instructions here: https://jmeter.apache.org/usermanual/get-started.html#install. To summarize it, just download it from their page, unzip, `cd` into the unzipped folder and run `./bin/jmeter.sh`.

Once the UI is running, go to `File > Open` and select one of the `.jmx` files available in this folder.

# Running the tests

The test suite is pre-configured to run the requests with a Random Controller approach, meaning that each user will randomly choose one of the requests and run it on each iteration. This is to achieve a closer simulation on the real production behavior.

To change the number of users and the ramp-up period, on the left panel go to `CRM_API`. It will open a section `Thread Group`. There you can change the fields `Number of Threads (users)` and `Ramp-up period (seconds)`.

`Number of Threads (users)` means the number of users that will be simultaneously hitting your API. This is constrained and affected by your machine resources, but JMeter does a great job taking the most out of it.

`Ramp-up period (seconds)` means the time it will take to reach the total number of users. e.g. if you set 200 users and a 20s ramp-up period, the test will take 20s to incrementally reach the number of 200 users and then keep it constant.

Press the Start button (a green "play" icon on the middle of the topbar) and it will start running your test suite. You can watch the reports being updated live.

## Results

The `Summary Report` gives a tabular overview of how each endpoint is performing. It includes important metrics like response time, error rate and throughput (req/s).

The `Results Tree` will show detailed information on each request that was made, including status and the response body.