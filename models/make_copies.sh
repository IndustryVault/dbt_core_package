#!/bin/bash
declare -a table_names
declare -i i=0
declare -i j=-1
declare -i add_to_array=0
test_value="DO NOT CHANGE"
exec < ../seeds/dictionary.csv || exit 1
read header # read (and ignore) the first line
while IFS=, read database_name version_name source_table_name stage_table_name source_column_name stage_column_name source_column_description stage_column_description external_column_name source_column_type stage_column_type sum_multiplier column_order is_obsolete group_number sum_field has_table_issue has_column_issue notes isLFN LFN_filter LFN_join delta_order; do
    add_to_array=1
    if [[ $1 == "public" ]]
    then
        if [[ "$isLFN" ==  "1" ]]
        then
            add_to_array=1
        else
            add_to_array=0
        fi
    fi

    if [[ $add_to_array == 1 ]]
    then
        if [[ "$test_value" != "$source_table_name" ]]
        then
            test_value="$source_table_name"
            table_names[i]="$source_table_name $stage_table_name"
            ((i++))
        fi
    fi
done
if [[ "$1" == "portfolio" ]] 
then
    j=1
elif [[ "$1" == "public" ]] 
then
    j=1
elif [[ "$1" == "historical" ]] 
then
    j=1
elif [[ "$1" == "input" ]] 
then
    j=0
fi

mkdir -p ./{$1,,}
if [[ $j > -1 ]]
then
    length=$i
    for (( i=0; i<${length}; i++ ));
    do
        arr=(${table_names[$i]})
        arr[0]=$(echo ${arr[0]} | tr '[:upper:]' '[:lower:]')
        arr[1]=$(echo ${arr[1]} | tr '[:upper:]' '[:lower:]')
        sed  "s/{@source_table_name}/${arr[0]}/;s/{@stage_table_name}/${arr[1]}/" $1_template.txt > ./$1/$1__${arr[j]}.sql
    done
fi
