/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "MergeTreeTool.h"
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

using namespace DB;

namespace local_engine
{
std::shared_ptr<DB::StorageInMemoryMetadata> buildMetaData(DB::NamesAndTypesList columns, ContextPtr context)
{
    std::shared_ptr<DB::StorageInMemoryMetadata> metadata = std::make_shared<DB::StorageInMemoryMetadata>();
    ColumnsDescription columns_description;
    for (const auto & item : columns)
    {
        columns_description.add(ColumnDescription(item.name, item.type));
    }
    metadata->setColumns(std::move(columns_description));
    metadata->partition_key.expression_list_ast = std::make_shared<ASTExpressionList>();
    metadata->sorting_key = KeyDescription::getSortingKeyFromAST(makeASTFunction("tuple"), metadata->getColumns(), context, {});
    metadata->primary_key.expression = std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>());
    return metadata;
}

std::unique_ptr<MergeTreeSettings> buildMergeTreeSettings()
{
    auto settings = std::make_unique<DB::MergeTreeSettings>();
    settings->set("min_bytes_for_wide_part", Field(0));
    settings->set("min_rows_for_wide_part", Field(0));
    return settings;
}

std::unique_ptr<SelectQueryInfo> buildQueryInfo(NamesAndTypesList & names_and_types_list)
{
    std::unique_ptr<SelectQueryInfo> query_info = std::make_unique<SelectQueryInfo>();
    query_info->query = std::make_shared<ASTSelectQuery>();
    auto syntax_analyzer_result = std::make_shared<TreeRewriterResult>(names_and_types_list);
    syntax_analyzer_result->analyzed_join = std::make_shared<TableJoin>();
    query_info->syntax_analyzer_result = syntax_analyzer_result;
    return query_info;
}


MergeTreeTable parseMergeTreeTableString(const std::string & info)
{
    ReadBufferFromString in(info);
    assertString("MergeTree;", in);
    MergeTreeTable table;
    readString(table.database, in);
    assertChar('\n', in);
    readString(table.table, in);
    assertChar('\n', in);
    readString(table.relative_path, in);
    assertChar('\n', in);
    readIntText(table.min_block, in);
    assertChar('\n', in);
    readIntText(table.max_block, in);
    assertChar('\n', in);
    assertEOF(in);
    return table;
}

std::string MergeTreeTable::toString() const
{
    WriteBufferFromOwnString out;
    writeString("MergeTree;", out);
    writeString(database, out);
    writeChar('\n', out);
    writeString(table, out);
    writeChar('\n', out);
    writeString(relative_path, out);
    writeChar('\n', out);
    writeIntText(min_block, out);
    writeChar('\n', out);
    writeIntText(max_block, out);
    writeChar('\n', out);
    return out.str();
}

}