from typing import List


class SqlHelper:
    @staticmethod
    def split(sql_script: str) -> List[str]:
        sql_list = []
        # 嵌套注释的层级数
        multi_comment_level = 0
        prefix = ""
        for line in sql_script.splitlines():
            # if line.strip().startswith('--'):
            line = line if not line.strip().startswith('--') else ''
            # 标记是否以双引号结尾
            has_terminated_double_quote = True
            # 标记是否以单引号结尾
            has_terminated_single_quote = True
            # 标记是否属于单行注释内容
            is_single_line_comment = False
            # 标记前一个字符是否是短横行 "-"
            was_pre_dash = False
            # 标记前一个字符是否是斜杆 "/"
            was_pre_slash = False
            # 标记前一个字符是否是星号 "*"
            was_pre_star = False
            last_semi_index = 0
            index = 0
            if len(prefix) > 0:
                prefix += "\n"
            for char in line:
                match char:
                    case "'":
                        if has_terminated_double_quote:
                            has_terminated_single_quote = not has_terminated_single_quote
                    case '"':
                        if has_terminated_single_quote:
                            has_terminated_double_quote = not has_terminated_double_quote
                    case '-':
                        if has_terminated_double_quote and has_terminated_single_quote:
                            if was_pre_dash:
                                is_single_line_comment = True
                        was_pre_dash = True
                    case '/':
                        if has_terminated_double_quote and has_terminated_single_quote:
                            # 如果'/'前面是'*'， 那么嵌套层级数-1
                            if was_pre_star:
                                multi_comment_level -= 1
                        was_pre_slash = True
                        was_pre_dash = False
                        was_pre_star = False
                    case '*':
                        if has_terminated_double_quote and has_terminated_single_quote:
                            # 如果'*'前面是'/'， 那么嵌套层级数+1
                            if was_pre_slash:
                                multi_comment_level += 1
                        was_pre_star = True
                        was_pre_dash = False
                        was_pre_slash = False
                    case ';':
                        # 当分号不在单引号内，不在双引号内，不属于单行注释，并且多行嵌套注释的层级数为0时，表示此分号应该作为分隔符进行划分
                        if (has_terminated_double_quote and
                                has_terminated_single_quote and
                                not is_single_line_comment and
                                multi_comment_level == 0):
                            sql_stmt = prefix + line[last_semi_index:index]
                            sql_list.append(sql_stmt)
                            prefix = ""
                            last_semi_index = index + 1
                    case _:
                        was_pre_dash = False
                        was_pre_slash = False
                        was_pre_star = False
                index += 1
            if last_semi_index != index or len(line) == 0:
                prefix = prefix + line[last_semi_index:]
        assert multi_comment_level == 0, (f"The number of nested levels of sql multi-line comments is not equal to 0: "
                                          f"{multi_comment_level}")
        return sql_list
