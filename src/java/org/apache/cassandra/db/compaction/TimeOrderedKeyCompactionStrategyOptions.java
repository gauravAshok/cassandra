/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

final class TimeOrderedKeyCompactionStrategyOptions
{
    private static final Logger logger = LoggerFactory.getLogger(TimeOrderedKeyCompactionStrategyOptions.class);

    protected static final String TOMBSTONE_COMPACTION_DELAY_UNIT_KEY = "tombstone_compaction_delay_unit";
    protected static final String TOMBSTONE_COMPACTION_DELAY_KEY = "tombstone_compaction_delay";

    // format: "1230,40.0" : 1230MB or 40%
    // default: "int_max:20" : 20% of the keys are tombstoned
    protected static final String DEFAULT_GARBAGE_SIZE_THRESHOLD_STR = Integer.MAX_VALUE + ",20";
    protected static final Pair<Long, Double> DEFAULT_GARBAGE_SIZE_THRESHOLD = parseSizeThreshold(DEFAULT_GARBAGE_SIZE_THRESHOLD_STR);
    protected static final String WINDOW_GARBAGE_SIZE_THRESHOLD_KEY = "window_garbage_size_threshold";
    protected static final String GLOBAL_GARBAGE_SIZE_THRESHOLD_KEY = "global_garbage_size_threshold";

    final TimeWindowCompactionStrategyOptions twcsOptions;
    final long tombstoneCompactionDelay;
    final TimeUnit tombstoneCompactionDelayUnit;

    final long windowCompactionSizeInMB;
    final double windowCompactionSizePercent;

    final long windowCompactionGlobalSizeInMB;
    final double windowCompactionGlobalSizePercent;

    TimeOrderedKeyCompactionStrategyOptions(Map<String, String> options)
    {

        this.twcsOptions = new TimeWindowCompactionStrategyOptions(options);

        String optionValue = options.get(TOMBSTONE_COMPACTION_DELAY_UNIT_KEY);
        tombstoneCompactionDelayUnit = optionValue == null ? twcsOptions.sstableWindowUnit : TimeUnit.valueOf(optionValue);

        optionValue = options.get(TOMBSTONE_COMPACTION_DELAY_KEY);
        tombstoneCompactionDelay = optionValue == null ? twcsOptions.sstableWindowSize : Integer.parseInt(optionValue);

        Pair<Long, Double> thresholdOption = parseSizeThreshold(options.get(WINDOW_GARBAGE_SIZE_THRESHOLD_KEY));
        windowCompactionSizeInMB = thresholdOption.left;
        windowCompactionSizePercent = thresholdOption.right;

        thresholdOption = parseSizeThreshold(options.get(GLOBAL_GARBAGE_SIZE_THRESHOLD_KEY));
        windowCompactionGlobalSizeInMB = thresholdOption.left;
        windowCompactionGlobalSizePercent = thresholdOption.right;
    }

    public static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions) throws ConfigurationException
    {
        uncheckedOptions = TimeWindowCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        TimeWindowCompactionStrategyOptions timeWindowOptions = new TimeWindowCompactionStrategyOptions(options);

        long compactionWindowSize, tombstoneDelay;

        String optionValue = options.get(TOMBSTONE_COMPACTION_DELAY_KEY);
        try
        {
            tombstoneDelay = optionValue == null ? timeWindowOptions.sstableWindowSize : Integer.parseInt(optionValue);
            if (tombstoneDelay < 1)
            {
                throw new ConfigurationException(String.format("%d must be greater than equal to 1 for %s", tombstoneDelay, TOMBSTONE_COMPACTION_DELAY_KEY));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, TOMBSTONE_COMPACTION_DELAY_KEY), e);
        }

        optionValue = options.get(TOMBSTONE_COMPACTION_DELAY_UNIT_KEY);
        try
        {
            if (optionValue != null)
            {
                if (!TimeWindowCompactionStrategyOptions.validWindowTimeUnits.contains(TimeUnit.valueOf(optionValue)))
                {
                    throw new ConfigurationException(String.format("%s is not valid for %s", optionValue, TOMBSTONE_COMPACTION_DELAY_UNIT_KEY));
                }
            }

            TimeUnit delayUnit = optionValue == null ? timeWindowOptions.sstableWindowUnit : TimeUnit.valueOf(optionValue);
            // convert to seconds
            tombstoneDelay = TimeUnit.SECONDS.convert(tombstoneDelay, delayUnit);
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException(String.format("%s is not valid for %s", optionValue, TOMBSTONE_COMPACTION_DELAY_UNIT_KEY), e);
        }

        // window sizes have been validated. Check for divisiblity

        compactionWindowSize = TimeUnit.SECONDS.convert(timeWindowOptions.sstableWindowSize, timeWindowOptions.sstableWindowUnit);

        if (tombstoneDelay < compactionWindowSize || tombstoneDelay % compactionWindowSize != 0)
        {
            throw new ConfigurationException(String.format(
                    "%s should be more than and divisible by %s when converted to seconds", TimeWindowCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_KEY, TOMBSTONE_COMPACTION_DELAY_KEY));
        }


        optionValue = options.get(WINDOW_GARBAGE_SIZE_THRESHOLD_KEY);
        try
        {
            Pair<Long, Double> threshold = parseSizeThreshold(optionValue);
            if (threshold.left < 1 || threshold.right <= 0.0)
            {
                throw new ConfigurationException(String.format("(%d, %f) must be greater than (0,0) for %s", threshold.left, threshold.right, WINDOW_GARBAGE_SIZE_THRESHOLD_KEY));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable for %s", optionValue, WINDOW_GARBAGE_SIZE_THRESHOLD_KEY), e);
        }

        optionValue = options.get(GLOBAL_GARBAGE_SIZE_THRESHOLD_KEY);
        try
        {
            Pair<Long, Double> threshold = parseSizeThreshold(optionValue);
            if (threshold.left < 1 || threshold.right <= 0.0)
            {
                throw new ConfigurationException(String.format("(%d, %f) must be greater than (0,0) for %s", threshold.left, threshold.right, GLOBAL_GARBAGE_SIZE_THRESHOLD_KEY));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable for %s", optionValue, GLOBAL_GARBAGE_SIZE_THRESHOLD_KEY), e);
        }

        uncheckedOptions.remove(TOMBSTONE_COMPACTION_DELAY_UNIT_KEY);
        uncheckedOptions.remove(TOMBSTONE_COMPACTION_DELAY_KEY);
        uncheckedOptions.remove(WINDOW_GARBAGE_SIZE_THRESHOLD_KEY);
        uncheckedOptions.remove(GLOBAL_GARBAGE_SIZE_THRESHOLD_KEY);

        return uncheckedOptions;
    }

    private static Pair<Long, Double> parseSizeThreshold(String option)
    {
        if(option == null || option.isEmpty()) {
            return DEFAULT_GARBAGE_SIZE_THRESHOLD;
        }

        String[] tokens = option.split(",");
        if (tokens.length != 2)
        {
            throw new IllegalArgumentException();
        }

        try
        {
            return Pair.create(Long.parseLong(tokens[0]), Double.parseDouble(tokens[1]));
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException(e);
        }
    }
}
