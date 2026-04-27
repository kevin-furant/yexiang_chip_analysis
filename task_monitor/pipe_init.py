#! /usr/bin/env python3
import json
from pathlib import Path
from typing import Any

"""
此步骤用于初始化流程,通过调取task_monitor/config目录下的配置文件,生成流程输入参数config.json
gCap.json: 所有的芯片相关的参数
chip_name_tozh.json: 芯片名称转换成中文名
config.json: 输出的config.json示例
"""
gCap_path = Path(__file__).parent.resolve() / 'config' / 'gCap.json'
chipName_map = Path(__file__).parent.resolve() / 'config' / 'chip_name_tozh.json'

def read_gCap(gcap_path: Path) -> dict:
    with open(gcap_path, 'r', encoding='utf-8') as f:
        gcap_data = json.load(f)
    return gcap_data

def read_chip_name_tozh(chip_name_tozh_path: Path = chipName_map) -> dict:
    with open(chip_name_tozh_path, 'r', encoding='utf-8') as f:
        chip_name_tozh_data = json.load(f)
    return chip_name_tozh_data

def generate_config(out_json_path: Path, **kwargs: dict[str, Any]) -> Path:
    gcap_data = read_gCap(gCap_path)
    for key, value in kwargs.items():
        if key in gcap_data:
            continue
        gcap_data[key] = value
    out_json_path.write_text(json.dumps(gcap_data, indent=4, ensure_ascii=False))
    return out_json_path