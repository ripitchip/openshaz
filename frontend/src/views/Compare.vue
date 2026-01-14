<script setup lang="ts">
import { ref } from 'vue'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Separator } from '@/components/ui/separator'
import { Input } from '@/components/ui/input'
import { backendUrl, addComparisonHistory, comparisonHistory } from '@/store/music'

const selectedFile = ref<File | null>(null)
const isAnalyzing = ref(false)
const errorMessage = ref('')
const similarSongs = ref<any[]>([])

const handleFileChange = (event: Event) => {
  const target = event.target as HTMLInputElement
  if (target.files && target.files.length > 0) {
    selectedFile.value = target.files[0] || null
    errorMessage.value = ''
  }
}

const handleGetSimilar = async () => {
  if (!selectedFile.value) {
    errorMessage.value = 'Please select a file'
    return
  }

  isAnalyzing.value = true
  errorMessage.value = ''
  similarSongs.value = []

  try {
    const formData = new FormData()
    formData.append('file', selectedFile.value)

    const response = await fetch(`${backendUrl.value}/get-similar`, {
      method: 'POST',
      body: formData
    })

    if (response.ok) {
      const data = await response.json()
      similarSongs.value = data.similar_songs || data.results || []
      addComparisonHistory(selectedFile.value.name, similarSongs.value)
    } else {
      errorMessage.value = `Error: ${response.status} ${response.statusText}`
    }
  } catch (err: any) {
    errorMessage.value = `Analysis failed: ${err.message}`
  } finally {
    isAnalyzing.value = false
  }
}
</script>

<template>
  <div class="max-w-5xl mx-auto">
    <div class="text-center mb-8">
      <h2 class="text-3xl font-bold text-blue-900 mb-2">Find Similar Tracks</h2>
      <p class="text-blue-700">Upload an audio file to find similar songs</p>
    </div>

    <Card class="bg-white shadow-2xl mb-8">
      <CardHeader>
        <CardTitle class="text-2xl text-blue-900">Upload Audio File</CardTitle>
        <CardDescription class="text-gray-600">
          Select an audio file to analyze and find similar tracks
        </CardDescription>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <label class="text-sm font-semibold text-gray-700">Audio File</label>
          <Input 
            type="file"
            @change="handleFileChange"
            accept="audio/*"
            class="h-12 text-lg cursor-pointer"
          />
          <p v-if="selectedFile" class="text-sm text-blue-600">
            Selected: {{ selectedFile.name }}
          </p>
        </div>
        <div v-if="errorMessage" class="p-4 bg-red-50 border border-red-200 rounded-lg">
          <p class="text-red-600">{{ errorMessage }}</p>
        </div>
        <Button 
          @click="handleGetSimilar" 
          :disabled="!selectedFile || isAnalyzing"
          class="w-full h-14 text-lg bg-linear-to-r from-blue-600 to-blue-700 hover:from-blue-700 hover:to-blue-800 text-white disabled:opacity-50"
        >
          <span class="mr-2">{{ isAnalyzing ? '⏳' : '🔍' }}</span> 
          {{ isAnalyzing ? 'Analyzing...' : 'Find Similar Tracks' }}
        </Button>
      </CardContent>
    </Card>

    <div v-if="similarSongs.length > 0">
      <Separator class="my-8 bg-gray-200" />
      
      <h3 class="text-2xl font-bold text-blue-900 mb-6">Similar Tracks Found</h3>
      
      <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        <Card 
          v-for="(song, index) in similarSongs" 
          :key="index"
          class="bg-white shadow-xl"
        >
          <div class="aspect-square bg-linear-to-br from-blue-400 to-purple-500 flex items-center justify-center">
            <span class="text-6xl">🎵</span>
          </div>
          <CardHeader>
            <CardTitle class="text-lg text-gray-900">{{ song.title || song.name || 'Unknown' }}</CardTitle>
            <CardDescription class="text-gray-600">{{ song.artist || 'Unknown Artist' }}</CardDescription>
          </CardHeader>
          <CardContent>
            <div class="space-y-2">
              <div v-if="song.similarity" class="flex justify-between items-center">
                <span class="text-sm font-semibold">Similarity</span>
                <Badge class="bg-blue-600">{{ Math.round(song.similarity * 100) }}%</Badge>
              </div>
              <div v-if="song.genre" class="flex justify-between items-center">
                <span class="text-sm font-semibold">Genre</span>
                <Badge variant="secondary">{{ song.genre }}</Badge>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>

    <div v-else-if="!isAnalyzing && selectedFile" class="text-center py-12">
      <div class="text-6xl mb-4">🎧</div>
      <p class="text-blue-900 text-xl">Click "Find Similar Tracks" to analyze</p>
    </div>

    <div class="mt-12" v-if="comparisonHistory.length">
      <Separator class="my-8 bg-gray-200" />
      <h3 class="text-2xl font-bold text-blue-900 mb-6">Previous Comparisons</h3>

      <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Card v-for="entry in comparisonHistory" :key="entry.id" class="bg-white shadow-md">
          <CardHeader>
            <CardTitle class="text-lg text-gray-900">{{ entry.filename || 'Unknown file' }}</CardTitle>
            <CardDescription>{{ new Date(entry.timestamp).toLocaleString() }}</CardDescription>
          </CardHeader>
          <CardContent>
            <div class="space-y-2">
              <div v-for="(song, idx) in entry.results" :key="idx" class="flex items-center justify-between border-b last:border-b-0 pb-2">
                <div>
                  <p class="text-sm font-semibold text-gray-900">{{ song.title || song.name || 'Unknown' }}</p>
                  <p class="text-xs text-gray-600">{{ song.artist || 'Unknown Artist' }}</p>
                </div>
                <Badge v-if="song.similarity" class="bg-blue-600">{{ Math.round(song.similarity * 100) }}%</Badge>
              </div>
              <p v-if="!entry.results || entry.results.length === 0" class="text-sm text-gray-500">No results stored.</p>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  </div>
</template>
