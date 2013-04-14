/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package com.noctarius.replikate.io.disk;

import java.nio.file.Path;

import com.noctarius.replikate.JournalConfiguration;

public class DiskJournalConfiguration<V>
    extends JournalConfiguration<V>
{

    private Path journalingPath;

    private int maxLogFileSize;

    public Path getJournalingPath()
    {
        return journalingPath;
    }

    public void setJournalingPath( Path journalingPath )
    {
        this.journalingPath = journalingPath;
    }

    public int getMaxLogFileSize()
    {
        return maxLogFileSize;
    }

    public void setMaxLogFileSize( int maxLogFileSize )
    {
        this.maxLogFileSize = maxLogFileSize;
    }

}
