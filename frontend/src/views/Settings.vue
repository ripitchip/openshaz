<script setup lang="ts">
import { ref } from 'vue'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Separator } from '@/components/ui/separator'
import { backendUrl, healthStatus, saveBackendUrl, checkHealth } from '@/store/music'

const localUrl = ref(backendUrl.value)

const handleSave = () => {
  saveBackendUrl(localUrl.value)
}

const handleTest = async () => {
  await checkHealth()
}
</script>

<template>
  <div class="max-w-2xl mx-auto">
    <div class="mb-6">
      <h2 class="text-3xl font-bold text-blue-900 mb-2">Settings</h2>
      <p class="text-blue-700">Configure your backend connection</p>
    </div>

    <Card class="bg-white shadow-lg">
      <CardHeader>
        <CardTitle class="text-gray-900">Backend Configuration</CardTitle>
        <CardDescription>Set the URL for your OpenShaz backend server</CardDescription>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <label class="text-sm font-medium text-gray-900">Backend URL</label>
          <Input 
            v-model="localUrl" 
            placeholder="http://localhost:8000"
            class="bg-white border-gray-300"
          />
        </div>

        <div class="flex gap-3">
          <Button 
            @click="handleSave"
            variant="ghost"
            class="bg-white! text-blue-700! border! border-blue-200! hover:bg-blue-50! hover:text-blue-800! active:bg-blue-100! active:scale-[0.99] shadow-sm"
          >
            Save
          </Button>
          <Button 
            @click="handleTest"
            variant="outline"
            class="bg-white! border! border-green-400! text-green-700! hover:bg-green-50! hover:text-green-800! active:bg-green-100! active:scale-[0.99]"
          >
            Test Connection
          </Button>
        </div>

        <Separator class="my-4" />

        <div class="space-y-3">
          <div class="flex items-center justify-between">
            <span class="text-sm font-medium text-gray-900">Connection Status</span>
            <Badge 
              :class="healthStatus.connected ? 'bg-green-500' : 'bg-red-500'"
              class="text-white"
            >
              {{ healthStatus.connected ? '✓ Connected' : '✗ Disconnected' }}
            </Badge>
          </div>

          <div v-if="healthStatus.message" class="text-sm text-gray-600">
            {{ healthStatus.message }}
          </div>
        </div>

        <Separator class="my-4" />

        <div class="space-y-2">
          <h3 class="text-sm font-medium text-gray-900">API Endpoints</h3>
          <div class="space-y-1 text-sm text-gray-600 font-mono">
            <div>Health: <span class="text-blue-600">{{ backendUrl }}/health</span></div>
            <div>Music: <span class="text-blue-600">{{ backendUrl }}/music</span></div>
            <div>Compare: <span class="text-blue-600">{{ backendUrl }}/compare</span></div>
          </div>
        </div>
      </CardContent>
    </Card>
  </div>
</template>
