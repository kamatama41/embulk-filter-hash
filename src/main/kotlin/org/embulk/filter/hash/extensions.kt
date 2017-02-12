package org.embulk.filter.hash

import org.embulk.config.ConfigSource
import org.embulk.config.TaskSource

inline fun <reified T : Any> ConfigSource.loadConfig() = loadConfig(T::class.java)!!

inline fun <reified T : Any> TaskSource.loadTask() = loadTask(T::class.java)!!
