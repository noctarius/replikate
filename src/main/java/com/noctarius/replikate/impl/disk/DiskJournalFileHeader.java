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
package com.noctarius.replikate.impl.disk;

class DiskJournalFileHeader {

    static final byte[] MAGIC_NUMBER = {(byte) 0xFE, (byte) 0xEE, (byte) 0xEE, (byte) 0xEF};

    private final int firstDataOffset;
    private final int maxLogFileSize;
    private final long logFileNumber;
    private final int version;
    private final byte type;

    DiskJournalFileHeader(int version, int maxLogFileSize, long logFileNumber, byte type) {
        this(version, maxLogFileSize, logFileNumber, type, DiskJournal.JOURNAL_FILE_HEADER_SIZE);
    }

    DiskJournalFileHeader(int version, int maxLogFileSize, long logFileNumber, byte type, int firstDataOffset) {
        this.version = version;
        this.maxLogFileSize = maxLogFileSize;
        this.logFileNumber = logFileNumber;
        this.type = type;
        this.firstDataOffset = firstDataOffset;
    }

    public int getVersion() {
        return version;
    }

    int getMaxLogFileSize() {
        return maxLogFileSize;
    }

    long getLogNumber() {
        return logFileNumber;
    }

    byte getType() {
        return type;
    }

    int getFirstDataOffset() {
        return firstDataOffset;
    }

}
