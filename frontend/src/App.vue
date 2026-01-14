<script setup lang="ts">
import { onMounted } from 'vue'
import { Button } from '@/components/ui/button'
import { useRoute, useRouter } from 'vue-router'
import { showSettings, checkHealth, healthStatus } from '@/store/music'

const route = useRoute()
const router = useRouter()

const isActive = (path: string) => {
  // Check if current path matches, or if we're at root and checking for /add
  if (route.path === path) return true
  if (route.path === '/' && path === '/add') return true
  return false
}

// Check health on mount
onMounted(() => {
  checkHealth()
  // Check health every 30 seconds
  setInterval(checkHealth, 30000)
})
</script>

<template>
  <div class="min-h-screen bg-white overflow-y-scroll">
    <nav class="bg-white shadow-md border-b border-gray-200 sticky top-0 z-50">
      <div class="container mx-auto px-6 py-4">
        <div class="flex items-center justify-between w-full gap-50">

          <div class="flex items-center gap-3">
            <div
              class="w-12 h-12 bg-linear-to-br from-blue-400 to-blue-600 rounded-2xl flex items-center justify-center shadow-lg">
              <span class="text-2xl">🎵</span>
            </div>
            <h1 class="text-2xl font-bold text-blue-900">OpenShaz</h1>
          </div>

          <div class="flex items-center gap-6">
            <div class="flex items-center gap-2">
              <div 
                :class="[
                  'w-3 h-3 rounded-full transition-all',
                  healthStatus.connected 
                    ? 'bg-green-500 shadow-lg shadow-green-500/50' 
                    : 'bg-red-500 shadow-lg shadow-red-500/50'
                ]">
              </div>
              <span class="text-xs font-medium text-gray-600">
                {{ healthStatus.connected ? 'Connected' : 'Disconnected' }}
              </span>
            </div>

            <div class="flex gap-4">
              <Button @click="router.push('/add')" variant="ghost"
                :class="isActive('/add') ? 'bg-linear-to-br! from-blue-400! to-blue-600! text-white! font-semibold w-32' : 'bg-white! text-blue-600! hover:text-blue-700! hover:bg-blue-50! w-32 border border-blue-50'">
                Add Music
              </Button>
              <Button @click="router.push('/compare')" variant="ghost"
                :class="isActive('/compare') ? 'bg-linear-to-br! from-blue-400! to-blue-600! text-white! font-semibold w-32' : 'bg-white! text-blue-600! hover:text-blue-700! hover:bg-blue-50! w-32 border border-blue-50'">
                Compare
              </Button>
              <Button v-if="showSettings" @click="router.push('/settings')" variant="ghost"
                :class="isActive('/settings') ? 'bg-linear-to-br! from-blue-400! to-blue-600! text-white! font-semibold w-32' : 'bg-white! text-blue-600! hover:text-blue-700! hover:bg-blue-50! w-32 border border-blue-50'">
                Settings
              </Button>
            </div>
          </div>
        </div>
      </div>
    </nav>

    <div class="container mx-auto px-6 py-8 max-w-7xl">
      <RouterView />
    </div>
  </div>
</template>

<style scoped></style>
