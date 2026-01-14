<script setup lang="ts">
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { musicList, selectedMusic, toggleSelection } from '@/store/music'
</script>

<template>
  <div class="max-w-6xl mx-auto">
    <div class="mb-6">
      <h2 class="text-3xl font-bold text-blue-900 mb-2">Your Music Library</h2>
      <p class="text-blue-700">{{ musicList.length }} tracks available</p>
    </div>
    
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
      <Card 
        v-for="music in musicList" 
        :key="music.id"
        class="bg-white hover:shadow-2xl transition-all cursor-pointer overflow-hidden group"
        @click="toggleSelection(music.id)"
        :class="{ 'ring-4 ring-blue-500 shadow-2xl': selectedMusic.includes(music.id) }"
      >
        <div class="aspect-square bg-linear-to-br from-blue-400 to-purple-500 flex items-center justify-center relative">
          <span class="text-6xl group-hover:scale-110 transition-transform">🎵</span>
          <Badge 
            v-if="selectedMusic.includes(music.id)" 
            class="absolute top-3 right-3 bg-blue-600"
          >
            ✓ Selected
          </Badge>
        </div>
        <CardHeader class="pb-3">
          <CardTitle class="text-lg text-gray-900 line-clamp-1">
            {{ music.title }}
          </CardTitle>
          <CardDescription class="text-gray-600">
            {{ music.artist }}
          </CardDescription>
        </CardHeader>
        <CardContent class="pt-0">
          <div class="flex flex-wrap gap-2">
            <Badge variant="secondary" class="text-xs">{{ music.genre }}</Badge>
            <Badge variant="outline" class="text-xs">{{ music.bpm }} BPM</Badge>
            <Badge variant="outline" class="text-xs">{{ music.key }}</Badge>
          </div>
        </CardContent>
      </Card>
    </div>
    
    <div v-if="musicList.length === 0" class="text-center py-20">
      <div class="text-6xl mb-4">🎵</div>
      <p class="text-blue-900 text-xl">Your library is empty</p>
      <p class="text-blue-700 mt-2">Add some tracks to get started</p>
    </div>
  </div>
</template>
