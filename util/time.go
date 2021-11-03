// @文件名: time
// @作者: 邓一鸣
// @创建时间: 2021/10/12 3:52 下午
// @描述:
package util

import "time"

func TimeNowUnix() float64 {
	return float64(time.Now().UnixMicro()) / 1e6
}
