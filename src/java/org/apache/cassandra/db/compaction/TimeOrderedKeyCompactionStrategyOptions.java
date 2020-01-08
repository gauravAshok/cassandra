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

import java.util.Map;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.Pair;

final class TimeOrderedKeyCompactionStrategyOptions
{
    protected static final String COMPACTION_MAX_SIZE_MB = "compaction_max_size_mb";
    protected static final int DEFAULT_COMPACTION_MAX_SIZE_MB = 64000;

    protected static final String SPLIT_FACTOR_KEY = "split_factor";
    protected static final int DEFAULT_SPLIT_FACTOR = 8;

    // format: "1230,40.0" : 1230MB or 40%
    // default: "int_max:20" : 20% of the keys are tombstoned
    protected static final String DEFAULT_GARBAGE_SIZE_THRESHOLD_STR = Integer.MAX_VALUE + ",20";
    protected static final Pair<Long, Double> DEFAULT_GARBAGE_SIZE_THRESHOLD = parseSizeThreshold(DEFAULT_GARBAGE_SIZE_THRESHOLD_STR);
    protected static final String WINDOW_GARBAGE_SIZE_THRESHOLD_KEY = "window_garbage_size_threshold";
    protected static final String GLOBAL_GARBAGE_SIZE_THRESHOLD_KEY = "global_garbage_size_threshold";

    final TimeWindowCompactionStrategyOptions twcsOptions;

    final long windowCompactionSizeInMB;
    final double windowCompactionSizePercent;

    final long windowCompactionGlobalSizeInMB;
    final double windowCompactionGlobalSizePercent;

    final int splitFactor;
    final int compactionMaxSizeMB;

    TimeOrderedKeyCompactionStrategyOptions(Map<String, String> options)
    {
        this.twcsOptions = new TimeWindowCompactionStrategyOptions(options);

        Pair<Long, Double> thresholdOption = parseSizeThreshold(options.get(WINDOW_GARBAGE_SIZE_THRESHOLD_KEY));
        this.windowCompactionSizeInMB = thresholdOption.left;
        this.windowCompactionSizePercent = thresholdOption.right;

        thresholdOption = parseSizeThreshold(options.get(GLOBAL_GARBAGE_SIZE_THRESHOLD_KEY));
        this.windowCompactionGlobalSizeInMB = thresholdOption.left;
        this.windowCompactionGlobalSizePercent = thresholdOption.right;

        String splitFactorOption = options.get(SPLIT_FACTOR_KEY);
        this.splitFactor = splitFactorOption == null ? DEFAULT_SPLIT_FACTOR : Integer.parseInt(splitFactorOption);

        String maxSizeOption = options.get(COMPACTION_MAX_SIZE_MB);
        this.compactionMaxSizeMB = maxSizeOption == null ? DEFAULT_COMPACTION_MAX_SIZE_MB : Integer.parseInt(maxSizeOption);
    }

    public static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions) throws ConfigurationException
    {
        uncheckedOptions = TimeWindowCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        String optionValue = options.get(WINDOW_GARBAGE_SIZE_THRESHOLD_KEY);
        try
        {
            Pair<Long, Double> threshold = parseSizeThreshold(optionValue);
            if (threshold.left < 0 || threshold.right < 0.0 || threshold.right > 100.0)
            {
                throw new ConfigurationException(String.format("(%d, %f) must be >= (0,0) for %s", threshold.left, threshold.right, WINDOW_GARBAGE_SIZE_THRESHOLD_KEY));
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
            if (threshold.left < 0 || threshold.right < 0.0 || threshold.right > 100.0)
            {
                throw new ConfigurationException(String.format("(%d, %f) must be >= (0,0) for %s", threshold.left, threshold.right, GLOBAL_GARBAGE_SIZE_THRESHOLD_KEY));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable for %s", optionValue, GLOBAL_GARBAGE_SIZE_THRESHOLD_KEY), e);
        }

        optionValue = options.get(SPLIT_FACTOR_KEY);
        if (optionValue != null)
        {
            try
            {
                int splitFactor = Integer.parseInt(optionValue);
                // there should be a split i.e. split > 1. split factor shouldnt be too large.
                if (splitFactor < 2 || splitFactor > 128)
                {
                    throw new ConfigurationException(String.format("%d should be in the range [2, 128] for %s", splitFactor, SPLIT_FACTOR_KEY));
                }
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s is not parsable for %s", optionValue, SPLIT_FACTOR_KEY));
            }
        }

        optionValue = options.get(COMPACTION_MAX_SIZE_MB);
        if (optionValue != null)
        {
            try
            {
                int compactionMaxSizeMB = Integer.parseInt(optionValue);
                if (compactionMaxSizeMB < 1)
                {
                    throw new ConfigurationException(String.format("%d should be greater than 0 for %s", compactionMaxSizeMB, COMPACTION_MAX_SIZE_MB));
                }
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s is not parsable for %s", optionValue, COMPACTION_MAX_SIZE_MB));
            }
        }

        uncheckedOptions.remove(COMPACTION_MAX_SIZE_MB);
        uncheckedOptions.remove(SPLIT_FACTOR_KEY);
        uncheckedOptions.remove(WINDOW_GARBAGE_SIZE_THRESHOLD_KEY);
        uncheckedOptions.remove(GLOBAL_GARBAGE_SIZE_THRESHOLD_KEY);

        return uncheckedOptions;
    }

    private static Pair<Long, Double> parseSizeThreshold(String option)
    {
        if (option == null || option.isEmpty())
        {
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
